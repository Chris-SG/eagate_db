package ddr_db

import (
	"fmt"
	"github.com/chris-sg/eagate_models/ddr_models"
	"github.com/jinzhu/gorm"
)

func AddSongs(db *gorm.DB, songs []ddr_models.Song) error {
	return db.Transaction(func(tx *gorm.DB) error {
		errCount := 0
		for _, song := range songs {
			if err := tx.Create(&song).Error; err != nil {
				errCount++
			}
		}

		if errCount != 0 {
			return fmt.Errorf("error adding songs to db, failed %d of %d times", errCount, len(songs))
		}

		return nil
	})
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
	return db.Transaction(func(tx *gorm.DB) error {
		errCount := 0
		for _, difficulty := range difficulties {
			if err := tx.Create(&difficulty).Error; err != nil {
				errCount++
			}
		}

		if errCount != 0 {
			return fmt.Errorf("error adding difficulties to db, failed %d of %d times", errCount, len(difficulties))
		}

		return nil
	})
}