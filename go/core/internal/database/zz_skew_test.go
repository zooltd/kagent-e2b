package database

import (
	"testing"
	"time"
)

func TestClockSkewZZ(t *testing.T) {
	db := setupTestDB(t)
	var dbNow time.Time
	for i := 0; i < 5; i++ {
		goNow := time.Now()
		if err := db.QueryRow(t.Context(), "SELECT clock_timestamp()").Scan(&dbNow); err != nil {
			t.Fatal(err)
		}
		t.Logf("delta(dbNow-goNow)=%v dbBehind=%v", dbNow.Sub(goNow), dbNow.Before(goNow))
	}
}
