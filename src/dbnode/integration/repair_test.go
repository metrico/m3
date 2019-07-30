// +build integration

// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/integration/generate"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

func TestRepair(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// Test setups
	log := xtest.NewLogger(t)
	retentionOpts := retention.NewOptions().
		SetRetentionPeriod(20 * time.Hour).
		SetBlockSize(2 * time.Hour).
		SetBufferPast(10 * time.Minute).
		SetBufferFuture(2 * time.Minute)
	nsOpts := namespace.NewOptions().
		SetRepairEnabled(true).
		SetRetentionOptions(retentionOpts)
	namesp, err := namespace.NewMetadata(testNamespaces[0], nsOpts)
	require.NoError(t, err)
	opts := newTestOptions(t).
		SetNamespaces([]namespace.Metadata{namesp})

	setupOpts := []bootstrappableTestSetupOptions{
		{disablePeersBootstrapper: true, enableRepairs: true},
		{disablePeersBootstrapper: true, enableRepairs: true},
		{disablePeersBootstrapper: true, enableRepairs: true},
	}
	setups, closeFn := newDefaultBootstrappableTestSetups(t, opts, setupOpts)
	defer closeFn()

	// Write test data alternating missing data for left/right nodes
	now := setups[0].getNowFn()
	blockSize := retentionOpts.BlockSize()

	node1Data := generate.BlocksByStart([]generate.BlockConfig{
		{IDs: []string{"foo"}, NumPoints: 90, Start: now.Add(-4 * blockSize)},
	})
	node2Data := generate.BlocksByStart([]generate.BlockConfig{
		{IDs: []string{"bar"}, NumPoints: 90, Start: now.Add(-4 * blockSize)},
	})

	allData := make(map[xtime.UnixNano]generate.SeriesBlock)
	for start, data := range node1Data {
		for _, series := range data {
			allData[start] = append(allData[start], series)
		}
	}
	for start, data := range node2Data {
		for _, series := range data {
			allData[start] = append(allData[start], series)
		}
	}
	// node3Data := generate.BlocksByStart([]generate.BlockConfig{
	// 	{IDs: []string{"baz"}, NumPoints: 90, Start: now.Add(-4 * blockSize)},
	// })
	// Make sure we have multiple blocks of data for multiple series to exercise
	// the grouping and aggregating logic in the client peer bootstrapping process
	// seriesMaps := generate.BlocksByStart([]generate.BlockConfig{
	// 	{IDs: []string{"foo", "baz"}, NumPoints: 90, Start: now.Add(-4 * blockSize)},
	// 	{IDs: []string{"foo", "baz"}, NumPoints: 90, Start: now.Add(-3 * blockSize)},
	// 	{IDs: []string{"foo", "baz"}, NumPoints: 90, Start: now.Add(-2 * blockSize)},
	// 	{IDs: []string{"foo", "baz"}, NumPoints: 90, Start: now.Add(-blockSize)},
	// 	{IDs: []string{"foo", "baz"}, NumPoints: 90, Start: now},
	// })
	// left := make(map[xtime.UnixNano]generate.SeriesBlock)
	// right := make(map[xtime.UnixNano]generate.SeriesBlock)
	// shouldMissData := false
	// appendSeries := func(target map[xtime.UnixNano]generate.SeriesBlock, start time.Time, s generate.Series) {
	// 	startNano := xtime.ToUnixNano(start)
	// 	if shouldMissData {
	// 		var dataWithMissing []generate.TestValue
	// 		for i := range s.Data {
	// 			if i%2 != 0 {
	// 				continue
	// 			}
	// 			dataWithMissing = append(dataWithMissing, s.Data[i])
	// 		}
	// 		target[startNano] = append(target[startNano], generate.Series{ID: s.ID, Data: dataWithMissing})
	// 	} else {
	// 		target[startNano] = append(target[startNano], s)
	// 	}
	// 	shouldMissData = !shouldMissData
	// }
	// for start, data := range seriesMaps {
	// 	for _, series := range data {
	// 		appendSeries(left, start.ToTime(), series)
	// 		appendSeries(right, start.ToTime(), series)
	// 	}
	// }
	require.NoError(t, writeTestDataToDisk(namesp, setups[0], node1Data, 0))
	require.NoError(t, writeTestDataToDisk(namesp, setups[1], node2Data, 0))

	// Start the first two servers with filesystem bootstrappers
	setups[:2].parallel(func(s *testSetup) {
		if err := s.startServer(); err != nil {
			panic(err)
		}
	})

	// Start the last server with peers and filesystem bootstrappers
	require.NoError(t, setups[2].startServer())
	log.Debug("servers are now up")

	// Stop the servers
	defer func() {
		setups.parallel(func(s *testSetup) {
			require.NoError(t, s.stopServer())
		})
		log.Debug("servers are now down")
	}()

	for _, s := range setups {
		s.setNowFn(s.getNowFn().Add(time.Minute))
	}
	time.Sleep(30 * time.Second)
	// Verify in-memory data match what we expect
	verifySeriesMaps(t, setups[0], namesp.ID(), allData)
	verifySeriesMaps(t, setups[1], namesp.ID(), allData)

	fmt.Println("^^^^^^^^^^^^^^")
	verifySeriesMaps(t, setups[2], namesp.ID(), allData)
}
