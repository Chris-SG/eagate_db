package db_builder

import (
	"github.com/chris-sg/eagate_models/ddr_models"
	"github.com/jinzhu/gorm"
)

func Create(db *gorm.DB) {
	db.AutoMigrate(&ddr_models.Song{}, &ddr_models.SongDifficulty{})
	db.Model(&ddr_models.Song{}).AddForeignKey("song_id", "ddrSongDifficulties(song_id)", "cascade", "cascade")
}