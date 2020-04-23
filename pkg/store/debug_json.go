// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"sort"

	"github.com/go-kit/kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DebugLocalJSONStore implements the store API against... files with stream of proto-based SeriesResponses in JSON format.
// Inefficient implementation for quick StoreAPI view.
type DebugLocalJSONStore struct {
	logger    log.Logger
	extLabels labels.Labels

	info   *storepb.InfoResponse
	mmaped []byte
	c      io.Closer

	// TODO(bwplotka): This is very naive in-memory DB. We can support much larger files, by
	// indexing labels, symbolizing strings and get chunk refs only without storing protobufs in memory.
	// For small debug purposes, this is good enough.
	series []*storepb.Series
}

// TODO(bwplotka): Add remote read so Prometheus users can use this.
func NewDebugLocalJSONStore(
	logger log.Logger,
	component component.StoreAPI,
	extLabels labels.Labels,
	path string,
	split bufio.SplitFunc,
) (*DebugLocalJSONStore, error) {
	f, err := fileutil.OpenMmapFile(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			runutil.CloseWithErrCapture(&err, f, "json file %s close", path)
		}
	}()

	s := &DebugLocalJSONStore{
		logger:    logger,
		extLabels: extLabels,
		mmaped:    f.Bytes(),
		c:         f,
		info: &storepb.InfoResponse{
			LabelSets: []storepb.LabelSet{
				{Labels: storepb.PromLabelsToLabelsUnsafe(extLabels)},
			},
			StoreType: component.ToProto(),
			MinTime:   math.MaxInt64,
			MaxTime:   math.MinInt64,
		},
	}

	// Do quick pass for in-mem index.
	content := s.mmaped
	contentStart := bytes.Index(content, []byte("{"))
	if contentStart != -1 {
		content = content[contentStart:]
	} else {
		contentStart = 0
	}

	if idx := bytes.LastIndex(content, []byte("}")); idx != -1 {
		content = content[:idx]
	}

	scanner := bufio.NewScanner(bytes.NewReader(content))
	scanner.Split(split)

	resp := &storepb.SeriesResponse{}
	for scanner.Scan() {
		fmt.Println(string(scanner.Bytes()))
		if err := proto.Unmarshal(scanner.Bytes(), resp); err != nil {
			return nil, errors.Wrapf(err, "unmarshal storepb.SeriesResponse frame of file %s", path)
		}
		series := resp.GetSeries()
		if series == nil {
			continue
		}

		// Sort chunks by MinTime for easier lookup. Find global max and min.
		sort.Slice(series.Chunks, func(i, j int) bool {
			if s.info.MinTime > series.Chunks[i].MinTime {
				s.info.MinTime = series.Chunks[i].MinTime
			}
			if s.info.MaxTime < series.Chunks[i].MaxTime {
				s.info.MaxTime = series.Chunks[i].MaxTime
			}
			cmp := series.Chunks[i].MinTime < series.Chunks[j].MaxTime
			if !cmp {
				logger.Log("unsorted chunks")
			}
			return cmp
		})
		s.series = append(s.series, series)
	}
	if err := scanner.Err(); err != nil {
		return nil, errors.Wrapf(err, "scanning file %s", path)
	}
	return s, nil
}

// GRPCCurlSplit allows to tokenize each streamed gRPC message from grpcurl tool.
func GRPCCurlSplit(data []byte, atEOF bool) (advance int, token []byte, err error) {
	var delim = []byte(`}
{`)
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if bytes.HasSuffix(data, delim) {
		return len(data) - 1, data[:len(data)-1], nil
	}
	// If we're at EOF, let's return all.
	if atEOF {
		return len(data), data, nil
	}
	return 1, data[0:1], nil
}

// Info returns store information about the Prometheus instance.
func (s *DebugLocalJSONStore) Info(_ context.Context, _ *storepb.InfoRequest) (*storepb.InfoResponse, error) {
	return s.info, nil
}

// Series returns all series for a requested time range and label matcher. The returned data may
// exceed the requested time bounds.
func (s *DebugLocalJSONStore) Series(r *storepb.SeriesRequest, srv storepb.Store_SeriesServer) error {
	match, newMatchers, err := matchesExternalLabels(r.Matchers, s.extLabels)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if !match {
		return nil
	}
	if len(newMatchers) == 0 {
		return status.Error(codes.InvalidArgument, errors.New("no matchers specified (excluding external labels)").Error())
	}
	matchers, err := translateMatchers(newMatchers)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	for _, series := range s.series {
		lbls := storepb.LabelsToPromLabelsUnsafe(series.Labels)
		var noMatch bool
		for _, m := range matchers {
			extValue := lbls.Get(m.Name)
			if extValue == "" {
				continue
			}
			if !m.Matches(extValue) {
				noMatch = true
				break
			}
		}
		if noMatch {
			continue
		}

		if err := srv.Send(storepb.NewSeriesResponse(series)); err != nil {
			return status.Error(codes.Aborted, err.Error())
		}
	}
	return nil
}

// LabelNames returns all known label names.
func (s *DebugLocalJSONStore) LabelNames(_ context.Context, _ *storepb.LabelNamesRequest) (
	*storepb.LabelNamesResponse, error,
) {
	// TODO(bwplotka): Consider precomputing.
	names := map[string]struct{}{}
	for _, series := range s.series {
		for _, l := range series.Labels {
			names[l.Name] = struct{}{}
		}
	}
	resp := &storepb.LabelNamesResponse{}
	for n := range names {
		resp.Names = append(resp.Names, n)
	}
	return resp, nil
}

// LabelValues returns all known label values for a given label name.
func (s *DebugLocalJSONStore) LabelValues(_ context.Context, r *storepb.LabelValuesRequest) (
	*storepb.LabelValuesResponse, error,
) {
	vals := map[string]struct{}{}
	for _, series := range s.series {
		lbls := storepb.LabelsToPromLabelsUnsafe(series.Labels)
		val := lbls.Get(r.Label)
		if val == "" {
			continue
		}
		vals[val] = struct{}{}
	}
	resp := &storepb.LabelValuesResponse{}
	for val := range vals {
		resp.Values = append(resp.Values, val)
	}
	return resp, nil
}

func (s *DebugLocalJSONStore) Close() (err error) {
	return s.Close()
}
