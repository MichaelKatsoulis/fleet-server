#!/usr/bin/env groovy
@Library('apm@current') _

pipeline {
  agent none
  environment {
    REPO = 'fleet-server'
    BASE_DIR = "src/github.com/elastic/${env.REPO}"
    SLACK_CHANNEL = '#elastic-agent-control-plane'
    NOTIFY_TO = 'fleet-server+build-package@elastic.co'
    JOB_GCS_BUCKET = credentials('gcs-bucket')
    JOB_GCS_CREDENTIALS = 'beats-ci-gcs-plugin'
    DOCKER_SECRET = 'secret/observability-team/ci/docker-registry/prod'
    DOCKER_REGISTRY = 'docker.elastic.co'
  }
  options {
    timeout(time: 2, unit: 'HOURS')
    buildDiscarder(logRotator(numToKeepStr: '100', artifactNumToKeepStr: '30', daysToKeepStr: '30'))
    timestamps()
    ansiColor('xterm')
    disableResume()
    durabilityHint('PERFORMANCE_OPTIMIZED')
    rateLimitBuilds(throttle: [count: 60, durationName: 'hour', userBoost: true])
    quietPeriod(10)
  }
  triggers {
    // disable upstream trigger on a PR basis
    upstream("Ingest-manager/fleet-server/${ env.JOB_BASE_NAME.startsWith('PR-') ? 'none' : env.JOB_BASE_NAME }")
  }
  stages {
    stage('Filter build') {
      options { skipDefaultCheckout() }
      agent { label 'ubuntu-20 && immutable' }
      when {
        beforeAgent true
        anyOf {
          triggeredBy cause: "IssueCommentCause"
          expression {
            def ret = isUserTrigger() || isUpstreamTrigger()
            if(!ret){
              currentBuild.result = 'NOT_BUILT'
              currentBuild.description = "The build has been skipped"
              currentBuild.displayName = "#${BUILD_NUMBER}-(Skipped)"
              echo("the build has been skipped due the trigger is a branch scan and the allowed ones are manual, GitHub comment, and upstream job")
            }
            return ret
          }
        }
      }
      environment {
        PATH = "${env.PATH}:${env.WORKSPACE}/bin"
        HOME = "${env.WORKSPACE}"
      }
      stages {
        stage('Checkout') {
          options { skipDefaultCheckout() }
          steps {
            pipelineManager([ cancelPreviousRunningBuilds: [ when: 'PR' ] ])
            deleteDir()
            gitCheckout(basedir: "${BASE_DIR}", githubNotifyFirstTimeContributor: false,
                        shallow: false, reference: "/var/lib/jenkins/.git-references/${REPO}.git")
            stash allowEmpty: true, name: 'source', useDefaultExcludes: false
            // set environment variables globally since they are used afterwards but GIT_BASE_COMMIT won't
            // be available until gitCheckout is executed.
            setEnvVar('URI_SUFFIX', "commits/${env.GIT_BASE_COMMIT}")
            // JOB_GCS_BUCKET contains the bucket and some folders, let's build the folder structure
            setEnvVar('PATH_PREFIX', "${JOB_GCS_BUCKET.contains('/') ? JOB_GCS_BUCKET.substring(JOB_GCS_BUCKET.indexOf('/') + 1) + '/' + env.URI_SUFFIX : env.URI_SUFFIX}")
            setEnvVar('IS_BRANCH_AVAILABLE', isBranchUnifiedReleaseAvailable(env.BRANCH_NAME))
            dir("${BASE_DIR}") {
              setEnvVar('VERSION', sh(label: 'Get version', script: 'make get-version', returnStdout: true)?.trim())
            }
          }
        }
        stage('Package') {
          options { skipDefaultCheckout() }
          matrix {
            agent {
              label "${PLATFORM}"
            }
            axes {
              axis {
                name 'PLATFORM'
                values 'ubuntu-20 && immutable', 'arm'
              }
            }
            stages {
              stage('Package') {
                options { skipDefaultCheckout() }
                environment {
                  PLATFORMS = "${isArm() ? 'linux/arm64' : ''}"
                  PACKAGES = "${isArm() ? 'docker' : ''}"
                  SNAPSHOT = 'true'
                }
                steps {
                  deleteDir()
                  unstash 'source'
                  dir("${BASE_DIR}"){
                    withMageEnv() {
                      sh(label: 'make release-manager-snapshot', script: 'make release-manager-snapshot')
                    }
                  }
                }
              }
              stage('Publish') {
                options { skipDefaultCheckout() }
                steps {
                  // Copy those files to another location with the sha commit to test them afterward.
                  googleStorageUpload(bucket: "gs://${JOB_GCS_BUCKET}/${URI_SUFFIX}",
                    credentialsId: "${JOB_GCS_CREDENTIALS}",
                    pathPrefix: "${BASE_DIR}/build/distributions/",
                    pattern: "${BASE_DIR}/build/distributions/**/*",
                    sharedPublicly: true,
                    showInline: true)
                }
              }
            }
          }
          post {
            failure {
              notifyStatus(subject: "[${env.REPO}@${env.BRANCH_NAME}] package failed.",
                           body: 'Contact the Productivity team [#observablt-robots] if you need further assistance.')
            }
          }
        }
        stage('DRA') {
          options { skipDefaultCheckout() }
          // The Unified Release process keeps moving branches as soon as a new
          // minor version is created, therefore old release branches won't be able
          // to use the release manager as their definition is removed.
          when {
            expression { return env.IS_BRANCH_AVAILABLE == "true" }
          }
          environment {
            DRA_OUTPUT = 'release-manager.out'
          }
          steps {
            googleStorageDownload(bucketUri: "gs://${JOB_GCS_BUCKET}/${URI_SUFFIX}/*",
                                  credentialsId: "${JOB_GCS_CREDENTIALS}",
                                  localDirectory: "${BASE_DIR}/build/distributions",
                                  pathPrefix: env.PATH_PREFIX)
            dir("${BASE_DIR}") {
              withMageEnv() {
                sh(label: 'create dependencies file', script: 'make release-manager-dependencies-snapshot')
              }
              dockerLogin(secret: env.DOCKER_SECRET, registry: env.DOCKER_REGISTRY)
              releaseManager(project: 'fleet-server',
                             version: env.VERSION,
                             type: 'snapshot',
                             artifactsFolder: 'build/distributions',
                             outputFile: env.DRA_OUTPUT)
            }
          }
          post {
            failure {
              notifyStatus(analyse: true,
                           file: "${BASE_DIR}/${env.DRA_OUTPUT}",
                           subject: "[${env.REPO}@${env.BRANCH_NAME}] The Daily releasable artifact failed.",
                           body: 'Contact the Release Platform team [#platform-release].')
            }
          }
        }
      }
    }
  }
  post {
    cleanup {
      notifyBuildResult(prComment: false)
    }
  }
}

def notifyStatus(def args = [:]) {
  def releaseManagerFile = args.get('file', '')
  def analyse = args.get('analyse', false)
  def subject = args.get('subject', '')
  def body = args.get('body', '')
  releaseManagerNotification(file: releaseManagerFile,
                             analyse: analyse,
                             slackChannel: "${env.SLACK_CHANNEL}",
                             slackColor: 'danger',
                             slackCredentialsId: 'jenkins-slack-integration-token',
                             to: "${env.NOTIFY_TO}",
                             subject: subject,
                             body: "Build: (<${env.RUN_DISPLAY_URL}|here>).\n ${body}")
}
