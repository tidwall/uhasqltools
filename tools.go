package uhasqltools

import (
	"errors"
	"strings"
	"time"

	"github.com/tidwall/uhatools"
)

// SQLResultSet ...
type SQLResultSet struct {
	ColumnNames []string
	Rows        [][]string
}

// SQLExec ...
func SQLExec(conn *uhatools.Conn, sql string) ([]SQLResultSet, error) {
	// Split the sql statement into two parts, the keyword and the remaining.
	var res []SQLResultSet
	var err error
	idx := strings.IndexByte(sql, ' ')
	if idx == -1 {
		res, err = getResultSets(conn.Do(sql))
	} else {
		keyword := sql[:idx]
		remain := strings.TrimSpace(sql[idx+1:])
		if remain == "" {
			res, err = getResultSets(conn.Do(sql))
		} else {
			res, err = getResultSets(conn.Do(keyword, remain))
		}
	}
	if err != nil {
		return nil, cleanUhahaErr(err)
	}
	return res, nil
}

func getResultSets(v interface{}, err error) ([]SQLResultSet, error) {
	vals, err := uhatools.Values(v, err)
	if err != nil {
		return nil, err
	}
	rss := make([]SQLResultSet, 0, len(vals))
	for _, v := range vals {
		var rs SQLResultSet
		vals2, err := uhatools.Values(v, err)
		if err != nil {
			return nil, err
		}
		for i, v := range vals2 {
			cols, err := uhatools.Strings(v, err)
			if err != nil {
				return nil, err
			}
			if i == 0 {
				rs.ColumnNames = cols
			} else {
				rs.Rows = append(rs.Rows, cols)
			}
		}
		rss = append(rss, rs)
	}
	return rss, nil
}

func cleanUhahaErr(err error) error {
	if err == nil {
		return err
	}
	errmsg := err.Error()
	if strings.HasPrefix(errmsg, "ERR ") {
		return errors.New(errmsg[4:])
	}
	return err
}

// SQLEscapeString ...
func SQLEscapeString(str string) string {
	return strings.ReplaceAll(str, "'", "''")
}

// SQLStringToTime ...
func SQLStringToTime(str string) time.Time {
	tm, _ := time.Parse("2006-01-02 15:04:05", str)
	return tm
}
