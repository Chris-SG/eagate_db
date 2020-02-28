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
	db.Model(&ddr_models.Song{}).Select("id").Pluck("id", &ids)
	//db.Select("song_id").Find(&ddr_models.Song{}).Pluck("song_id", &ids)
	return ids
}

func RetrieveSongsById(db *gorm.DB, ids []string) []ddr_models.Song {
	var songs []ddr_models.Song
	db.Model(&ddr_models.Song{}).Select([]string{"id", "name", "artist"}).Where("id IN (?)", ids).Scan(&songs)
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
	db.Model(&ddr_models.SongDifficulty{}).Where("difficulty_value > -1").Scan(&difficulties)
	return difficulties
}

func AddPlayerDetails(db *gorm.DB, playerDetails ddr_models.PlayerDetails) error {
	err := db.Save(&playerDetails).Error
	if err != nil {
		return err
	}
	return nil
}

func AddPlaycountDetails(db *gorm.DB, playcountDetails ddr_models.Playcount) error {
	err := db.Save(&playcountDetails).Error
	if err != nil {
		return err
	}
	return nil
}

func RetrieveDdrPlayerDetailsByEaGateUser(db *gorm.DB, eaUser string) (*ddr_models.PlayerDetails, error) {
	results := make([]*ddr_models.PlayerDetails, 0)
	db.Model(&ddr_models.PlayerDetails{}).Where("eagate_user = ?", eaUser).Scan(&results)
	if len(results) == 0 {
		return nil, fmt.Errorf("could not find user for username %s", eaUser)
	}
	if len(results) > 1 {
		return nil, fmt.Errorf("multiple ddr users found for username %s", eaUser)
	}
	return results[0], nil
}

func AddSongStatistics(db *gorm.DB, songStatistics []ddr_models.SongStatistics, code int) error {
	allSongStatistics := RetrieveSongStatistics(db, code)
	errCount := 0
	songStatisticsToAddOrUpdate := make([]ddr_models.SongStatistics, 0)
	for _, scrapedStatistic := range songStatistics {
		matched := false
		for _, dbStatistic := range allSongStatistics {
			if scrapedStatistic == dbStatistic {
				matched = true
				break
			}
		}
		if !matched {
			songStatisticsToAddOrUpdate = append(songStatisticsToAddOrUpdate, scrapedStatistic)
		}
	}

	addToDb := func(dbConn *gorm.DB, statJob <-chan ddr_models.SongStatistics, doneJob chan<- bool) {
		for stat := range statJob {
			err := dbConn.Save(&stat).Error
			doneJob <- err == nil
		}
	}

	jobs := make(chan ddr_models.SongStatistics, len(songStatisticsToAddOrUpdate))
	done := make(chan bool, len(songStatisticsToAddOrUpdate))

	for w := 1; w <= eagate_db.GetIdleConnectionLimit(); w++ {
		go addToDb(db, jobs, done)
	}

	for _, s := range songStatisticsToAddOrUpdate {
		jobs <- s
	}
	close(jobs)

	for result := 1; result <= len(songStatisticsToAddOrUpdate); result++ {
		if <-done == false {
			errCount++
		}
	}
	close(done)

	if errCount != 0 {
		return fmt.Errorf("error adding statistics to db, failed %d of %d times", errCount, len(songStatisticsToAddOrUpdate))
	}

	return nil
}

func RetrieveSongStatistics(db *gorm.DB, code int) []ddr_models.SongStatistics {
	var statistics []ddr_models.SongStatistics
	db.Model(&ddr_models.SongStatistics{}).Where("player_code = ?", code).Scan(&statistics)
	return statistics
}

func AddScores(db *gorm.DB, scores []ddr_models.Score) error {
	errCount := 0

	addToDb := func(dbConn *gorm.DB, scoreJob <-chan ddr_models.Score, doneJob chan<- bool) {
		for score := range scoreJob {
			err := dbConn.Save(&score).Error
			doneJob <- err == nil
		}
	}

	jobs := make(chan ddr_models.Score, len(scores))
	done := make(chan bool, len(scores))

	for w := 1; w <= eagate_db.GetIdleConnectionLimit(); w++ {
		go addToDb(db, jobs, done)
	}

	for _, s := range scores {
		jobs <- s
	}
	close(jobs)

	for result := 1; result <= len(scores); result++ {
		if <-done == false {
			errCount++
		}
	}
	close(done)

	if errCount != 0 {
		return fmt.Errorf("error adding scores to db, failed %d of %d times", errCount, len(scores))
	}

	return nil
}