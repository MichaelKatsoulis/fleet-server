// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/elastic/fleet-server/v7/internal/pkg/bulk"
	"github.com/elastic/fleet-server/v7/internal/pkg/cache"
	"github.com/elastic/fleet-server/v7/internal/pkg/config"
	"github.com/elastic/fleet-server/v7/internal/pkg/dl"
	"github.com/elastic/fleet-server/v7/internal/pkg/logger"
	"github.com/elastic/fleet-server/v7/internal/pkg/model"

	"github.com/hashicorp/go-version"
	"github.com/julienschmidt/httprouter"
	"github.com/miolini/datacounter"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const (
	kHintsMod = "hints"
)

type HintHundler struct {
	verCon version.Constraints
	cfg    *config.Server
	bulker bulk.Bulk
	cache  cache.Cache
}

func NewHintHundler(verCon version.Constraints, cfg *config.Server, bulker bulk.Bulk, c cache.Cache) (*HintHundler, error) {

	return &HintHundler{
		verCon: verCon,
		cfg:    cfg,
		bulker: bulker,
		cache:  c,
	}, nil

}

func (rt Router) handleHints(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	start := time.Now()
	agentId := ps.ByName("id")

	reqID := r.Header.Get(logger.HeaderRequestID)

	zlog := log.With().
		Str(ECSHTTPRequestID, reqID).
		Str("mod", kHintsMod).
		Logger()

	// Error in the scope for deferred rolback function check
	var err error

	var resp *HintsResponse
	resp, err = rt.hh.handleHints(&zlog, w, r, agentId)

	if err != nil {
		resp := NewHTTPErrResp(err)

		zlog.WithLevel(resp.Level).
			Err(err).
			Int(ECSHTTPResponseCode, resp.StatusCode).
			Int64(ECSEventDuration, time.Since(start).Nanoseconds()).
			Msg("fail hints")

		if rerr := resp.Write(w); rerr != nil {
			zlog.Error().Err(rerr).Msg("fail writing error response")
		}
		return
	}

	if err = wResponse(zlog, w, resp, start); err != nil {
		zlog.Error().
			Err(err).
			Int64(ECSEventDuration, time.Since(start).Nanoseconds()).
			Msg("fail write response")
	}
}

func (hh *HintHundler) handleHints(zlog *zerolog.Logger, w http.ResponseWriter, r *http.Request, id string) (*HintsResponse, error) {

	//key, err := authAPIKey(r, hh.bulker, hh.cache)
	//if err != nil {
	//	return nil, err
	//}
	//
	//// Pointer is passed in to allow UpdateContext by child function
	//zlog.UpdateContext(func(ctx zerolog.Context) zerolog.Context {
	//	return ctx.Str(LogHintsAPIKeyID, key.Id)
	//})

	//ver, err := validateUserAgent(*zlog, r, hh.verCon)
	//if err != nil {
	//	return nil, err
	//}

	return hh.processRequest(*zlog, w, r, id)
}

func (hh *HintHundler) processRequest(zlog zerolog.Logger, w http.ResponseWriter, r *http.Request, id string) (*HintsResponse, error) {

	// Validate that an enrollment record exists for a key with this id.
	//erec, err := hh.fetchEnrollmentKeyRecord(r.Context(), enrollmentAPIKeyID)
	//if err != nil {
	//	return nil, err
	//}

	body := r.Body

	readCounter := datacounter.NewReaderCounter(body)

	// Parse the request body
	req, err := decodeHintsRequest(readCounter)
	if err != nil {
		return nil, err
	}

	return hh._create(r.Context(), zlog, req, id)
}

func (hh *HintHundler) _create(ctx context.Context, zlog zerolog.Logger, req *HintsRequest, id string) (*HintsResponse, error) {
	req.AgentId = id
	err := createHint(ctx, hh.bulker, id, *req)
	if err != nil {
		return nil, err
	}

	resp := HintsResponse{
		Action: "created",
	}

	return &resp, nil
}

func wResponse(zlog zerolog.Logger, w http.ResponseWriter, resp *HintsResponse, start time.Time) error {

	data, err := json.Marshal(resp)
	if err != nil {
		return errors.Wrap(err, "marshal hintsResponse")
	}

	numWritten, err := w.Write(data)
	cntEnroll.bodyOut.Add(uint64(numWritten))

	if err != nil {
		return errors.Wrap(err, "fail send hints response")
	}

	zlog.Info().
		Int64(ECSEventDuration, time.Since(start).Nanoseconds()).
		Msg("Fleet Hints Added")

	return nil
}

func createHint(ctx context.Context, bulker bulk.Bulk, id string, hints HintsRequest) error {
	data, err := json.Marshal(hints)
	if err != nil {
		return err
	}

	_, err = bulker.Create(ctx, dl.FleetHints, id, data, bulk.WithRefresh())
	if err != nil {
		return err
	}
	return nil
}

func (hh *HintHundler) fetchEnrollmentKeyRecord(ctx context.Context, id string) (*model.EnrollmentAPIKey, error) {

	if key, ok := hh.cache.GetEnrollmentApiKey(id); ok {
		return &key, nil
	}

	// Pull API key record from .fleet-enrollment-api-keys
	rec, err := dl.FindEnrollmentAPIKey(ctx, hh.bulker, dl.QueryEnrollmentAPIKeyByID, dl.FieldAPIKeyID, id)
	if err != nil {
		return nil, errors.Wrap(err, "FindEnrollmentAPIKey")
	}

	if !rec.Active {
		return nil, ErrInactiveEnrollmentKey
	}

	cost := int64(len(rec.APIKey))
	hh.cache.SetEnrollmentApiKey(id, rec, cost)

	return &rec, nil
}

func decodeHintsRequest(data io.Reader) (*HintsRequest, error) {

	var req HintsRequest
	decoder := json.NewDecoder(data)
	if err := decoder.Decode(&req); err != nil {
		return nil, errors.Wrap(err, "decode hints request")
	}

	return &req, nil
}
