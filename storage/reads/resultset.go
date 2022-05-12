package reads

import (
	"context"

	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
)

type multiShardCursors interface {
	createCursor(row SeriesRow) cursors.Cursor
}

type resultSet struct {
	ctx          context.Context
	seriesCursor SeriesCursor
	seriesRow    SeriesRow
	arrayCursors multiShardCursors
}

// TODO(jsternberg): The range is [start, end) for this function which is consistent
// with the documented interface for datatypes.ReadFilterRequest. This function should
// be refactored to take in a datatypes.ReadFilterRequest similar to the other
// ResultSet functions.
func NewFilteredResultSet(ctx context.Context, start, end int64, seriesCursor SeriesCursor) ResultSet {
	return &resultSet{
		ctx:          ctx,
		seriesCursor: seriesCursor,
		arrayCursors: newMultiShardArrayCursors(ctx, start, end, true),
	}
}

func (r *resultSet) Err() error { return nil }

// Close closes the result set. Close is idempotent.
func (r *resultSet) Close() {
	if r == nil {
		return // Nothing to do.
	}
	r.seriesRow.Query = nil
	r.seriesCursor.Close()
}

// Next returns true if there are more results available.
func (r *resultSet) Next() bool {
	if r == nil {
		return false
	}

	seriesRow := r.seriesCursor.Next()
	if seriesRow == nil {
		return false
	}

	r.seriesRow = *seriesRow

	return true
}

func (r *resultSet) Cursor() cursors.Cursor {
	return r.arrayCursors.createCursor(r.seriesRow)
}

func (r *resultSet) Tags() models.Tags {
	return r.seriesRow.Tags
}

// Stats returns the stats for the underlying cursors.
// Available after resultset has been scanned.
func (r *resultSet) Stats() cursors.CursorStats { return r.seriesRow.Query.Stats() }

type limitResultSet struct {
	ResultSet
	blockLimit  int
	seriesLimit int
}

func NewLimitResultSet(rs ResultSet, blockLimit, seriesLimit int) ResultSet {
	return &limitResultSet{
		ResultSet:   rs,
		blockLimit:  blockLimit,
		seriesLimit: seriesLimit,
	}
}

func (r *limitResultSet) Next() bool {
	if r.seriesLimit == 0 {
		return false
	}
	r.seriesLimit--
	return r.ResultSet.Next()
}

func (r *limitResultSet) Cursor() cursors.Cursor {
	cur := r.ResultSet.Cursor()
	if cur == nil {
		return cur
	}
	return newBlockLimitArrayCursor(cur, r.blockLimit)
}
