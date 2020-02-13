package db_builder

import (
	"fmt"
	"github.com/chris-sg/eagate_models/ddr_models"
	"github.com/chris-sg/eagate_models/user_models"
	"github.com/jinzhu/gorm"
)

func Create(db *gorm.DB) {
	createUserTables(db)
	createDdrTables(db)

	createDdrConstraints(db)
}

func createUserTables(db *gorm.DB) {
	err := db.AutoMigrate(&user_models.User{}).Error
	if err != nil {
		fmt.Printf("error in AutoMigration: %s\n", err)
	}
}

func createDdrTables(db *gorm.DB) {
	err := db.AutoMigrate(&ddr_models.Song{}, &ddr_models.SongDifficulty{}, &ddr_models.PlayerDetails{}).
			  Error
	if err != nil {
		fmt.Printf("error in AutoMigration: %s\n", err)
	}
}

func createDdrConstraints(db *gorm.DB) {
	err := db.Model(&ddr_models.SongDifficulty{}).
		AddForeignKey("song_id", "public.\"ddrSongs\"(song_id)", "CASCADE", "CASCADE").
		Error
	if err != nil {
		fmt.Printf("error in FK creation: %s\n", err)
	}

	err = db.Model(&ddr_models.PlayerDetails{}).
		      AddForeignKey("eagate_user", "public.\"eaGateUser\"(account_name)", "RESTRICT", "RESTRICT").
			  Error
	if err != nil {
		fmt.Printf("error in FK creation: %s\n", err)
	}
}