package db_builder

import (
	"github.com/chris-sg/eagate_models/api_models"
	"github.com/chris-sg/eagate_models/ddr_models"
	"github.com/chris-sg/eagate_models/user_models"
	"github.com/golang/glog"
	"github.com/jinzhu/gorm"
)

func Create(db *gorm.DB) {
	glog.Infoln("creating db")
	CreateTables(db)
	CreateConstraints(db)
}

func CreateTables(db *gorm.DB) {
	glog.Infoln("creating db tables")
	createUserTables(db)
	createApiTables(db)
	createDdrTables(db)
}

func CreateConstraints(db *gorm.DB) {
	glog.Infoln("creating db constraints")
	createDdrConstraints(db)
}

func createUserTables(db *gorm.DB) {
	err := db.AutoMigrate(&user_models.User{}).Error
	if err != nil {
		glog.Warningf("automigration for user table user_models.User failed: %s\n", err.Error())
	}
}

func createApiTables(db *gorm.DB) {
	err := db.AutoMigrate(&api_models.AutomaticJob{}).Error
	if err != nil {
		glog.Warningf("automigration for api table api_models.AutomaticJob failed: %s\n", err.Error())
	}
}

func createDdrTables(db *gorm.DB) {
	err := db.AutoMigrate(&ddr_models.Song{}, &ddr_models.SongDifficulty{},
						  &ddr_models.PlayerDetails{}, &ddr_models.Playcount{},
						  &ddr_models.Score{}, ddr_models.SongStatistics{},
						  &ddr_models.WorkoutData{}).
			  Error
	if err != nil {
		glog.Warningf("automigration for ddr tables failed: %s\n", err.Error())
	}
}

func createDdrConstraints(db *gorm.DB) {
	err := db.Model(&ddr_models.SongDifficulty{}).
		AddForeignKey("song_id", "public.\"ddrSongs\"(id)", "CASCADE", "CASCADE").
		Error
	if err != nil {
		glog.Warningf("fk creation for ddr_models.SongDifficulty error: %s\n", err.Error())
	}

	err = db.Model(&ddr_models.PlayerDetails{}).
		      AddForeignKey("eagate_user", "public.\"eaGateUser\"(account_name)", "RESTRICT", "RESTRICT").
			  Error
	if err != nil {
		glog.Warningf("fk creation for ddr_models.PlayerDetails error: %s\n", err.Error())
	}

	err = db.Model(&ddr_models.Playcount{}).
		AddForeignKey("player_code", "public.\"ddrPlayerDetails\"(code)", "RESTRICT", "RESTRICT").
		Error
	if err != nil {
		glog.Warningf("fk creation for ddr_models.Playcount error: %s\n", err.Error())
	}

	err = db.Model(&ddr_models.WorkoutData{}).
		AddForeignKey("player_code", "public.\"ddrPlayerDetails\"(code)", "RESTRICT", "RESTRICT").
		Error
	if err != nil {
		glog.Warningf("fk creation for ddr_models.WorkoutData error: %s\n", err.Error())
	}

	err = db.Model(&ddr_models.SongStatistics{}).
		AddForeignKey("song_id,mode,difficulty", "public.\"ddrSongDifficulties\"(song_id,mode,difficulty)", "RESTRICT", "RESTRICT").
		Error
	if err != nil {
		glog.Warningf("fk creation for ddr_models.SongStatistics error: %s\n", err.Error())
	}

	err = db.Model(&ddr_models.SongStatistics{}).
		AddForeignKey("player_code", "public.\"ddrPlayerDetails\"(code)", "RESTRICT", "RESTRICT").
		Error
	if err != nil {
		glog.Warningf("fk creation for ddr_models.SongStatistics error: %s\n", err.Error())
	}

	err = db.Model(&ddr_models.Score{}).
		AddForeignKey("song_id,mode,difficulty", "public.\"ddrSongDifficulties\"(song_id,mode,difficulty)", "RESTRICT", "RESTRICT").
		Error
	if err != nil {
		glog.Warningf("fk creation for ddr_models.Score error: %s\n", err.Error())
	}

	err = db.Model(&ddr_models.Score{}).
		AddForeignKey("player_code", "public.\"ddrPlayerDetails\"(code)", "RESTRICT", "RESTRICT").
		Error
	if err != nil {
		glog.Warningf("fk creation for ddr_models.Score error: %s\n", err.Error())
	}
}