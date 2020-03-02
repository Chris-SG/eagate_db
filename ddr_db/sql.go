package ddr_db

import (
	"fmt"
	"github.com/chris-sg/eagate_models/ddr_models"
	"github.com/jinzhu/gorm"
	"github.com/lib/pq"
	"strconv"
)

const maxBatchSize = 100

func AddSongs(db *gorm.DB, songs []ddr_models.Song) error {
	currentIds := RetrieveSongIds(db)
	for i := len(songs)-1; i >= 0; i-- {
		for _, id := range currentIds {
			if id == songs[i].Id {
				songs = append(songs[:i], songs[i+1:]...)
				break
			}
		}
	}
	fmt.Printf("%d songs to add\n", len(songs))
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

	for _, completeStatement := range statements {
		fmt.Println(completeStatement)
		db.Exec(completeStatement)
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

func RetrieveSongsWithCovers(db *gorm.DB, ids []string) []ddr_models.Song {
	var songs []ddr_models.Song
	db.Model(&ddr_models.Song{}).Where("id IN (?)", ids).Scan(&songs)
	return songs
}


func AddSongDifficulties(db *gorm.DB, difficulties []ddr_models.SongDifficulty) error {
	allSongDifficulties := RetrieveSongDifficulties(db)
	fmt.Printf("range %d across %d", len(difficulties), len(allSongDifficulties))
	for i := len(difficulties)-1; i >= 0; i-- {
		for _, dbDifficulty := range allSongDifficulties {
			if difficulties[i] == dbDifficulty {
				difficulties = append(difficulties[:i], difficulties[i+1:]...)
				break
			}
		}
	}

	fmt.Printf("%d difficulties to add.\n", len(difficulties))

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

	for _, completeStatement := range statements {
		fmt.Println(completeStatement)
		db.Exec(completeStatement)
	}
	return nil
}

func RetrieveAllSongDifficulties(db *gorm.DB) []ddr_models.SongDifficulty {
	var difficulties []ddr_models.SongDifficulty
	db.Model(&ddr_models.SongDifficulty{}).Scan(&difficulties)
	return difficulties
}

func RetrieveValidSongDifficulties(db *gorm.DB) []ddr_models.SongDifficulty {
	var difficulties []ddr_models.SongDifficulty
	db.Model(&ddr_models.SongDifficulty{}).Where("difficulty_value > -1").Scan(&difficulties)
	return difficulties
}

func RetrieveSongDifficultiesById(db *gorm.DB, ids []string) []ddr_models.SongDifficulty {
	var difficulties []ddr_models.SongDifficulty
	db.Model(&ddr_models.SongDifficulty{}).Where("song_id IN (?)", ids).Scan(&difficulties)
	return difficulties
}

func AddPlayerDetails(db *gorm.DB, playerDetails ddr_models.PlayerDetails) error {
	fmt.Println(playerDetails)
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

func RetrieveDdrPlayerDetailsByCode(db *gorm.DB, code int) (*ddr_models.PlayerDetails, error) {
	results := make([]*ddr_models.PlayerDetails, 0)
	db.Model(&ddr_models.PlayerDetails{}).Where("code = ?", code).Scan(&results)
	if len(results) == 0 {
		return nil, fmt.Errorf("could not find user for code %s", code)
	}
	return results[0], nil
}

func RetrieveLatestPlaycountDetails(db *gorm.DB, playerCode int) *ddr_models.Playcount {
	pc := make([]*ddr_models.Playcount, 0)
	db.Model(&ddr_models.Playcount{}).Where("player_code = ?", playerCode).Order("playcount DESC", true).First(&pc)
	if len(pc) == 0 {
		return nil
	}
	return pc[0]
}

func AddSongStatistics(db *gorm.DB, songStatistics []ddr_models.SongStatistics, code int) error {
	allSongStatistics := RetrieveSongStatistics(db, code)
	for i := len(songStatistics)-1; i >= 0; i-- {
		for _, dbStatistic := range allSongStatistics {
			if songStatistics[i] == dbStatistic {
				songStatistics = append(songStatistics[:i], songStatistics[i+1:]...)
				break
			}
		}
	}

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

	for _, completeStatement := range statements {
		db.Exec(completeStatement)
	}

	return nil
}

func RetrieveSongStatistics(db *gorm.DB, code int) []ddr_models.SongStatistics {
	var statistics []ddr_models.SongStatistics
	db.Model(&ddr_models.SongStatistics{}).Where("player_code = ?", code).Scan(&statistics)
	return statistics
}

func AddScores(db *gorm.DB, scores []ddr_models.Score) error {
	fmt.Printf("%d scores\n", len(scores))

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

	for _, completeStatement := range statements {
		fmt.Println(completeStatement)
		db.Exec(completeStatement)
	}

	return nil
}