package ddr_db

import (
	"fmt"
	"github.com/chris-sg/eagate_models/ddr_models"
	"github.com/golang/glog"
	"github.com/jinzhu/gorm"
	"github.com/lib/pq"
	"strconv"
	"strings"
)

const maxBatchSize = 100

func AddSongs(db *gorm.DB, songs []ddr_models.Song) error {
	glog.Infof("AddSongs: %d songs to process\n", len(songs))
	currentIds := RetrieveSongIds(db)
	for i := len(songs)-1; i >= 0; i-- {
		for _, id := range currentIds {
			if id == songs[i].Id {
				songs = append(songs[:i], songs[i+1:]...)
				break
			}
		}
	}
	glog.Infof("AddSongs: %d new songs\n", len(songs))

	batchCount := 0
	processedCount := 0
	statements := make([]string, 0)
	var statement string
	statementBegin := `INSERT INTO public."ddrSongs" VALUES `
	statementEnd := ` ON CONFLICT DO NOTHING;`
	for i := len(songs)-1; i >= 0; i-- {
		statement = fmt.Sprintf("%s ('%s', '%s', '%s', '%s')", statement, songs[i].Id, songs[i].Name, songs[i].Artist, songs[i].Image)
		songs = songs[:len(songs)-1]
		batchCount++
		processedCount++
		if batchCount == maxBatchSize || i == 0 {
			statement = fmt.Sprintf("%s%s%s", statementBegin, statement, statementEnd)
			statements = append(statements, statement)
			statement = ""
		} else {
			statement = fmt.Sprintf("%s,", statement)
		}
	}

	totalRowsAffected := int64(0)
	for _, completeStatement := range statements {
		err := db.Exec(completeStatement).Error
		if err != nil {
			glog.Errorf("AddSongs failed executing statement: %s\n", err.Error())
		} else {
			totalRowsAffected += db.RowsAffected
		}
	}
	glog.Infof("AddSongs: %d rows affected", totalRowsAffected)
	return nil
}

func RetrieveSongIds(db *gorm.DB) []string {
	glog.Infoln("RetrieveSongIds")
	var ids []string
	err := db.Model(&ddr_models.Song{}).Select("id").Pluck("id", &ids).Error
	if err != nil {
		glog.Errorf("RetrieveSongIds failed: %s\n", err.Error())
	}
	//db.Select("song_id").Find(&ddr_models.Song{}).Pluck("song_id", &ids)
	return ids
}

func RetrieveSongsById(db *gorm.DB, ids []string) []ddr_models.Song {
	glog.Infof("RetrieveSongsByIds for %d ids\n", len(ids))
	var songs []ddr_models.Song
	err := db.Model(&ddr_models.Song{}).Select([]string{"id", "name", "artist"}).Where("id IN (?)", ids).Scan(&songs).Error
	if err != nil {
		glog.Errorf("RetrieveSongsById failed: %s\n", err.Error())
	}
	return songs
}

func RetrieveOrderedSongsById(db *gorm.DB, ids []string, ordering string) []ddr_models.Song {
	glog.Infof("RetrieveOrderedSongsByIds for %d ids, ordering %s\n", len(ids), ordering)
	var songs []ddr_models.Song
	err := db.Model(&ddr_models.Song{}).Select([]string{"id", "name", "artist"}).Where("id IN (?)", ids).Order(ordering).Scan(&songs).Error
	if err != nil {
		glog.Errorf("RetrieveOrderedSongsById failed: %s\n", err.Error())
	}
	return songs
}

func RetrieveSongsWithCovers(db *gorm.DB, ids []string) []ddr_models.Song {
	glog.Infof("RetrieveSongsWithCovers for %d ids\n", len(ids))
	var songs []ddr_models.Song
	err := db.Model(&ddr_models.Song{}).Where("id IN (?)", ids).Scan(&songs).Error
	if err != nil {
		glog.Errorf("RetrieveSongsWithCovers failed: %s\n", err.Error())
	}
	return songs
}


func AddSongDifficulties(db *gorm.DB, difficulties []ddr_models.SongDifficulty) error {
	glog.Infof("AddSongDifficulties for %d difficulties\n", len(difficulties))
	allSongDifficulties := RetrieveAllSongDifficulties(db)
	for i := len(difficulties)-1; i >= 0; i-- {
		for _, dbDifficulty := range allSongDifficulties {
			if difficulties[i] == dbDifficulty {
				difficulties = append(difficulties[:i], difficulties[i+1:]...)
				break
			}
		}
	}
	glog.Infof("AddSongDifficulties for %d new or updated difficulties\n", len(difficulties))
	batchCount := 0
	processedCount := 0
	statements := make([]string, 0)
	var statement string
	statementBegin := `INSERT INTO public."ddrSongDifficulties" VALUES `
	statementEnd := ` ON CONFLICT (song_id, mode, difficulty) DO UPDATE SET difficulty_value=EXCLUDED.difficulty_value;`
	for i, _ := range difficulties {
		statement = fmt.Sprintf("%s ('%s', '%s', '%s', %d)",
			statement,
			difficulties[i].SongId,
			difficulties[i].Mode,
			difficulties[i].Difficulty,
			difficulties[i].DifficultyValue)

		batchCount++
		processedCount++
		if batchCount == maxBatchSize || processedCount >= len(difficulties) {
			statement = fmt.Sprintf("%s%s%s", statementBegin, statement, statementEnd)
			statements = append(statements, statement)
			statement = ""
		} else {
			statement = fmt.Sprintf("%s,", statement)
		}
	}

	totalRowsAffected := int64(0)
	for _, completeStatement := range statements {
		err := db.Exec(completeStatement).Error
		if err != nil {
			glog.Errorf("AddSongDifficulties failed statement: %s\n", err.Error())
		} else {
			totalRowsAffected += db.RowsAffected
		}
	}
	glog.Infof("AddSongDifficulties: %d rows affected\n", totalRowsAffected)
	return nil
}

func RetrieveAllSongDifficulties(db *gorm.DB) []ddr_models.SongDifficulty {
	glog.Infoln("RetrieveAllSongDifficulties")
	var difficulties []ddr_models.SongDifficulty
	err := db.Model(&ddr_models.SongDifficulty{}).Scan(&difficulties).Error
	if err != nil {
		glog.Errorf("RetrieveAllSongDifficulties error: %s\n", err.Error())
	}
	return difficulties
}

func RetrieveValidSongDifficulties(db *gorm.DB) []ddr_models.SongDifficulty {
	glog.Infoln("RetrieveValidSongDifficulties")
	var difficulties []ddr_models.SongDifficulty
	err := db.Model(&ddr_models.SongDifficulty{}).Where("difficulty_value > -1").Scan(&difficulties).Error
	if err != nil {
		glog.Errorf("RetrieveValidSongDifficulties error: %s\n", err.Error())
	}
	return difficulties
}

func RetrieveSongDifficultiesById(db *gorm.DB, ids []string) []ddr_models.SongDifficulty {
	glog.Infoln("RetrieveSongDifficultiesById")
	var difficulties []ddr_models.SongDifficulty
	err := db.Model(&ddr_models.SongDifficulty{}).Where("song_id IN (?)", ids).Scan(&difficulties).Error
	if err != nil {
		glog.Errorf("RetrieveSongDifficultiesById error: %s\n", err.Error())
	}
	return difficulties
}

func AddPlayerDetails(db *gorm.DB, playerDetails ddr_models.PlayerDetails) error {
	glog.Infof("AddPlayerDetails for %s (code %d)\n", playerDetails.EaGateUser, playerDetails.Code)
	err := db.Save(&playerDetails).Error
	if err != nil {
		glog.Errorf("AddPlayerDetails failed: %s\n", err.Error())
		return err
	}
	return nil
}

func AddPlaycountDetails(db *gorm.DB, playcountDetails ddr_models.Playcount) error {
	glog.Infof("AddPlaycountDetails for code %d\n", playcountDetails.PlayerCode)
	err := db.Save(&playcountDetails).Error
	if err != nil {
		glog.Errorf("AddPlaycountDetails failed: %s\n", err.Error())
		return err
	}
	return nil
}

func RetrieveDdrPlayerDetailsByEaGateUser(db *gorm.DB, eaUser string) (*ddr_models.PlayerDetails, error) {
	glog.Infof("RetrieveDdrPlayerDetailsByEaGateUser for eaUser %s\n", eaUser)
	eaUser = strings.ToLower(eaUser)
	results := make([]*ddr_models.PlayerDetails, 0)
	err := db.Model(&ddr_models.PlayerDetails{}).Where("eagate_user = ?", eaUser).Scan(&results).Error
	if err != nil {
		glog.Errorf("RetrieveDdrPlayerDetailsByEaGateUser failed for user %s: %s\n", eaUser, err.Error())
	}
	if len(results) == 0 {
		glog.Errorf("RetrieveDdrPlayerDetailsByEaGateUser failed for user %s: could not find user for username\n", eaUser)
		return nil, fmt.Errorf("could not find user for username %s", eaUser)
	}
	if len(results) > 1 {
		glog.Errorf("RetrieveDdrPlayerDetailsByEaGateUser failed for user %s: multiple ddr users found for username\n", eaUser)
		return nil, fmt.Errorf("multiple ddr users found for username %s", eaUser)
	}
	return results[0], nil
}

func RetrieveDdrPlayerDetailsByCode(db *gorm.DB, code int) (*ddr_models.PlayerDetails, error) {
	glog.Infof("RetrieveDdrPlayerDetailsByCode for code %d\n", code)
	results := make([]*ddr_models.PlayerDetails, 0)
	err := db.Model(&ddr_models.PlayerDetails{}).Where("code = ?", code).Scan(&results).Error
	if err != nil {
		glog.Errorf("RetrieveDdrPlayerDetailsByCode failed for code %d: %s\n", code, err.Error())
	}
	if len(results) == 0 {
		glog.Errorf("RetrieveDdrPlayerDetailsByEaGateUser failed for code %d: no users found\n", code)
		return nil, fmt.Errorf("could not find user for code %s", code)
	}
	return results[0], nil
}

func RetrieveLatestPlaycountDetails(db *gorm.DB, playerCode int) *ddr_models.Playcount {
	glog.Infof("RetrieveLatestPlaycountDetails for playerCode %d\n", playerCode)
	pc := make([]*ddr_models.Playcount, 0)
	err := db.Model(&ddr_models.Playcount{}).Where("player_code = ?", playerCode).Order("playcount DESC", true).First(&pc).Error
	if err != nil {
		glog.Errorf("RetrieveLatestPlaycountDetails failed for playerCode %d: %s\n", playerCode, err.Error())
	}
	if len(pc) == 0 {
		return nil
	}
	return pc[0]
}

func AddSongStatistics(db *gorm.DB, songStatistics []ddr_models.SongStatistics, code int) error {
	glog.Infof("AddSongStatistics for playerCode %d (%d statistics)\n", code, len(songStatistics))
	allSongStatistics := RetrieveAllSongStatistics(db, code)
	for i := len(songStatistics)-1; i >= 0; i-- {
		for _, dbStatistic := range allSongStatistics {
			if songStatistics[i] == dbStatistic {
				songStatistics = append(songStatistics[:i], songStatistics[i+1:]...)
				break
			}
		}
	}
	glog.Infof("%d unique statistics for playerCode %d\n", len(songStatistics), code)

	batchCount := 0
	processedCount := 0
	statements := make([]string, 0)
	var statement string
	statementBegin := `INSERT INTO public."ddrSongStatistics" VALUES `
	statementEnd := ` ON CONFLICT (song_id, mode, difficulty, player_code) DO UPDATE SET ` +
		`score_record=EXCLUDED.score_record, ` +
		`clear_lamp=EXCLUDED.clear_lamp, ` +
		`rank=EXCLUDED.rank, ` +
		`playcount=EXCLUDED.playcount, ` +
		`clearcount=EXCLUDED.clearcount, ` +
		`maxcombo=EXCLUDED.maxcombo, ` +
		`lastplayed=EXCLUDED.lastplayed;`
	for i, _ := range songStatistics {
		statement = fmt.Sprintf("%s (%d, '%s', '%s', %d, %d, %d, '%s', '%s', '%s', '%s', %d)",
			statement,
			songStatistics[i].BestScore,
			songStatistics[i].Lamp,
			songStatistics[i].Rank,
			songStatistics[i].PlayCount,
			songStatistics[i].ClearCount,
			songStatistics[i].MaxCombo,
			pq.FormatTimestamp(songStatistics[i].LastPlayed),
			songStatistics[i].SongId,
			songStatistics[i].Mode,
			songStatistics[i].Difficulty,
			songStatistics[i].PlayerCode)

		batchCount++
		processedCount++
		if batchCount == maxBatchSize || processedCount >= len(songStatistics) {
			statement = fmt.Sprintf("%s%s%s", statementBegin, statement, statementEnd)
			statements = append(statements, statement)
			statement = ""
		} else {
			statement = fmt.Sprintf("%s,", statement)
		}
	}

	totalRowsAffected := int64(0)
	for _, completeStatement := range statements {
		err := db.Exec(completeStatement).Error
		if err != nil {
			glog.Errorf("AddSongStatistics failed for statement: %s\n", err.Error())
		} else {
			totalRowsAffected += db.RowsAffected
		}
	}
	glog.Infof("AddSongStatistics for playerCode %d: %d rows affected\n", code, totalRowsAffected)
	return nil
}

func RetrieveAllSongStatistics(db *gorm.DB, code int) []ddr_models.SongStatistics {
	glog.Info("RetrieveAllSongStatistics for player code %d\n", code)
	var statistics []ddr_models.SongStatistics
	err := db.Model(&ddr_models.SongStatistics{}).Where("player_code = ?", code).Scan(&statistics).Error
	if err != nil {
		glog.Errorf("RetrieveAllSongStatistics failed: %s\n", err.Error())
	}
	return statistics
}

func RetrieveSongStatisticsForSongsIds(db *gorm.DB, code int, songIds []string) []ddr_models.SongStatistics {
	glog.Info("RetrieveSongStatisticsForSongIds for player code %d (%d song ids)\n", code, len(songIds))
	var statistics []ddr_models.SongStatistics
	err := db.Model(&ddr_models.SongStatistics{}).Where("player_code = ? AND song_id IN (?)", code, songIds).Scan(&statistics).Error
	if err != nil {
		glog.Errorf("RetrieveSongStatisticsForSongsIds failed: %s\n", err.Error())
	}
	return statistics
}

func AddScores(db *gorm.DB, scores []ddr_models.Score) error {
	glog.Info("AddScores with %d scores\n", len(scores))
	batchCount := 0
	processedCount := 0
	statements := make([]string, 0)
	var statement string
	statementBegin := `INSERT INTO public."ddrScores" VALUES `
	statementEnd := ` ON CONFLICT DO NOTHING;`
	for i, _ := range scores {
		statement = fmt.Sprintf("%s (%d, '%s', '%s', '%s', '%s', '%s', %d)",
			statement,
			scores[i].Score,
			strconv.FormatBool(scores[i].ClearStatus),
			pq.FormatTimestamp(scores[i].TimePlayed),
			scores[i].SongId,
			scores[i].Mode,
			scores[i].Difficulty,
			scores[i].PlayerCode)

		batchCount++
		processedCount++
		if batchCount == maxBatchSize || processedCount >= len(scores) {
			statement = fmt.Sprintf("%s%s%s", statementBegin, statement, statementEnd)
			statements = append(statements, statement)
			statement = ""
		} else {
			statement = fmt.Sprintf("%s,", statement)
		}
	}

	totalRowsAffected := int64(0)
	for _, completeStatement := range statements {
		err := db.Exec(completeStatement).Error
		if err != nil {
			glog.Errorf("AddScores statement failed: %s\n", err.Error())
		} else {
			totalRowsAffected += db.RowsAffected
		}
	}
	glog.Infof("AddScores: %d rows affected\n", totalRowsAffected)

	return nil
}

func RetrieveScores(db *gorm.DB, code int, id string, mode string, difficulty string) (scores []ddr_models.Score) {
	glog.Infof("RetrieveScores for player code %d, song id %s, mode %s, difficulty %s\n", code, id, mode, difficulty)
	err := db.Model(&ddr_models.Score{}).Where("player_code = ? AND song_id = ? AND mode = ? AND difficulty = ?", code, id, mode, difficulty).Scan(&scores).Error
	if err != nil {
		glog.Errorf("RetrieveScores failed: %s\n", err.Error())
	}
	glog.Infof("Retrieved %d scores (player code %d)", len(scores), code)
	return
}

func AddWorkoutData(db *gorm.DB, workoutData []ddr_models.WorkoutData) {
	glog.Infof("AddWorkoutData: %d data points\n", len(workoutData))
	processedCount := 0
	var statement string
	statementBegin := `INSERT INTO public."ddrWorkoutData" VALUES `
	statementEnd := ` ON CONFLICT (date, player_code) DO UPDATE SET playcount=EXCLUDED.playcount, kcal=EXCLUDED.kcal;`
	for i, _ := range workoutData {
		statement = fmt.Sprintf("%s ('%s', '%d', '%f', %d)",
			statement,
			pq.FormatTimestamp(workoutData[i].Date),
			workoutData[i].PlayCount,
			workoutData[i].Kcal,
			workoutData[i].PlayerCode)

		processedCount++
		if processedCount >= len(workoutData) {
			statement = fmt.Sprintf("%s%s%s", statementBegin, statement, statementEnd)
		} else {
			statement = fmt.Sprintf("%s,", statement)
		}
	}

	err := db.Exec(statement).Error
	if err != nil {
		glog.Errorf("AddWorkoutData failed: %s\n", err.Error())
	}
	glog.Infof("AddWorkoutData: %d rows affected\n", db.RowsAffected)
}

func RetrieveWorkoutData(db *gorm.DB, code int) (workoutData []ddr_models.WorkoutData) {
	glog.Infof("RetrieveWorkoutData for player code %d\n", code)
	err := db.Model(&ddr_models.WorkoutData{}).Where("player_code = ?", code).Scan(&workoutData).Error
	if err != nil {
		glog.Errorf("RetrieveWorkoutData failed: %s\n", err.Error())
	}
	glog.Infof("RetrieveWorkoutData for player code %d: %d data points\n", code, len(workoutData))
	return
}