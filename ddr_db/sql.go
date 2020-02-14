package ddr_db

import (
	"fmt"
	"github.com/chris-sg/eagate_db"
	"github.com/chris-sg/eagate_models/ddr_models"
	"github.com/jinzhu/gorm"
)

func AddSongs(db *gorm.DB, songs []ddr_models.Song) error {
	errCount := 0
	addToDb := func(dbConn *gorm.DB, diffJob <-chan ddr_models.Song, doneJob chan<- bool) {
		for diff := range diffJob {
			fmt.Println(diff)
			err := dbConn.Save(&diff).Error
			doneJob <- err == nil
		}
	}

	jobs := make(chan ddr_models.Song, len(songs))
	done := make(chan bool, len(songs))

	for w := 1; w <= eagate_db.GetIdleConnectionLimit(); w++ {
		go addToDb(db, jobs, done)
	}

	for _, song := range songs {
		jobs <- song
	}
	close(jobs)

	for result := 1; result <= len(songs); result++ {
		if <-done == false {
			errCount++
		}
	}
	close(done)

	if errCount != 0 {
		return fmt.Errorf("error adding difficulties to db, failed %d of %d times", errCount, len(songs))
	}

	return nil
}

func RetrieveSongIds(db *gorm.DB) []string {
	var ids []string
	db.Model(&ddr_models.Song{}).Select("song_id").Pluck("song_id", &ids)
	//db.Select("song_id").Find(&ddr_models.Song{}).Pluck("song_id", &ids)
	return ids
}

func RetrieveSongsById(db *gorm.DB, ids []string) []ddr_models.Song {
	var songs []ddr_models.Song
	db.Model(&ddr_models.Song{}).Where("song_id IN (?)", ids).Scan(&songs)
	return songs
}


func AddSongDifficulties(db *gorm.DB, difficulties []ddr_models.SongDifficulty) error {
	allSongDifficulties := RetrieveSongDifficulties(db)
	errCount := 0
	songDifficultiesToAddOrUpdate := make([]ddr_models.SongDifficulty, 0)
	for _, scrapedDifficulty := range difficulties {
		matched := false
		for _, dbDifficulty := range allSongDifficulties {
			if scrapedDifficulty == dbDifficulty {
				matched = true
				break
			}
		}
		if !matched {
			songDifficultiesToAddOrUpdate = append(songDifficultiesToAddOrUpdate, scrapedDifficulty)
		}
	}

	addToDb := func(dbConn *gorm.DB, diffJob <-chan ddr_models.SongDifficulty, doneJob chan<- bool) {
		for diff := range diffJob {
			fmt.Println(diff)
			err := dbConn.Save(&diff).Error
			doneJob <- err == nil
		}
	}

	jobs := make(chan ddr_models.SongDifficulty, len(songDifficultiesToAddOrUpdate))
	done := make(chan bool, len(songDifficultiesToAddOrUpdate))

	for w := 1; w <= eagate_db.GetIdleConnectionLimit(); w++ {
		go addToDb(db, jobs, done)
	}

	for _, diff := range songDifficultiesToAddOrUpdate {
		jobs <- diff
	}
	close(jobs)

	for result := 1; result <= len(songDifficultiesToAddOrUpdate); result++ {
		if <-done == false {
			errCount++
		}
	}
	close(done)

	if errCount != 0 {
		return fmt.Errorf("error adding difficulties to db, failed %d of %d times", errCount, len(songDifficultiesToAddOrUpdate))
	}

	return nil
}

func RetrieveSongDifficulties(db *gorm.DB) []ddr_models.SongDifficulty {
	var difficulties []ddr_models.SongDifficulty
	db.Model(&ddr_models.SongDifficulty{}).Scan(&difficulties)
	return difficulties
}

func AddPlayerDetails(db *gorm.DB, playerDetails ddr_models.PlayerDetails) error {
	err := db.Save(&playerDetails).Error
	if err != nil {
		return err
	}
	return nil
}