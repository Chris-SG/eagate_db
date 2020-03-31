package db_builder

import (
	"github.com/chris-sg/eagate_models/api_models"
	"github.com/chris-sg/eagate_models/ddr_models"
	"github.com/chris-sg/eagate_models/user_models"
	"github.com/golang/glog"
	"github.com/jinzhu/gorm"
)

type DbMigrator interface {
	Create()
	CreateTables()
	CreateConstraints()

	createUserTables()
	createApiTables()
	createDdrTables()
	createDdrConstraints()
}

func CreateDbMigratorPostgres(db *gorm.DB) DbMigratorPostgres {
	return DbMigratorPostgres{db}
}

type DbMigratorPostgres struct {
	db *gorm.DB
}

func (migrator DbMigratorPostgres) Create() {
	glog.Infoln("creating db")
	migrator.CreateTables()
	migrator.CreateConstraints()
}

func (migrator DbMigratorPostgres) CreateTables() {
	glog.Infoln("creating db tables")
	migrator.createUserTables()
	migrator.createApiTables()
	migrator.createDdrTables()
}

func (migrator DbMigratorPostgres) CreateConstraints() {
	glog.Infoln("creating db constraints")
	migrator.createDdrConstraints()
}

func (migrator DbMigratorPostgres) createUserTables() {
	errs := migrator.db.AutoMigrate(&user_models.User{}).GetErrors()
	if errs != nil && len(errs) > 0 {
		glog.Warningln("automigration for user table user_models.User contained errors:")
		for _, err := range errs {
			glog.Warningf("\t%s\n", err.Error())
		}
	}
}

func (migrator DbMigratorPostgres) createApiTables() {
	errs := migrator.db.AutoMigrate(&api_models.AutomaticJob{}).GetErrors()

	if errs != nil && len(errs) > 0 {
		glog.Warningln("automigration for api table api_models.AutomaticJob contained errors:")
		for _, err := range errs {
			glog.Warningf("\t%s\n", err.Error())
		}
	}
}

func (migrator DbMigratorPostgres) createDdrTables() {
	errs := migrator.db.AutoMigrate(&ddr_models.Song{}, &ddr_models.SongDifficulty{},
						  &ddr_models.PlayerDetails{}, &ddr_models.Playcount{},
						  &ddr_models.Score{}, ddr_models.SongStatistics{},
						  &ddr_models.WorkoutData{}).
			  GetErrors()
	if errs != nil && len(errs) > 0 {
		glog.Warningln("automigration for ddr tables contained errors:")
		for _, err := range errs {
			glog.Warningf("\t%s\n", err.Error())
		}
	}
}

func (migrator DbMigratorPostgres) createDdrConstraints() {
	errs := migrator.db.Model(&ddr_models.SongDifficulty{}).
		AddForeignKey("song_id", "public.\"ddrSongs\"(id)", "CASCADE", "CASCADE").
		GetErrors()
	if errs != nil && len(errs) > 0 {
		glog.Warningln("fk creation for ddr_models.SongDifficulty contained errors:")
		for _, err := range errs {
			glog.Warningf("\t%s\n", err.Error())
		}
	}

	errs = migrator.db.Model(&ddr_models.PlayerDetails{}).
		      AddForeignKey("eagate_user", "public.\"eaGateUser\"(account_name)", "RESTRICT", "RESTRICT").
			  GetErrors()
	if errs != nil && len(errs) > 0 {
		glog.Warningln("fk creation for ddr_models.PlayerDetails contained errors:")
		for _, err := range errs {
			glog.Warningf("\t%s\n", err.Error())
		}
	}

	errs = migrator.db.Model(&ddr_models.Playcount{}).
		AddForeignKey("player_code", "public.\"ddrPlayerDetails\"(code)", "RESTRICT", "RESTRICT").
		GetErrors()
	if errs != nil && len(errs) > 0 {
		glog.Warningln("fk creation for ddr_models.Playcount contained errors:")
		for _, err := range errs {
			glog.Warningf("\t%s\n", err.Error())
		}
	}

	errs = migrator.db.Model(&ddr_models.WorkoutData{}).
		AddForeignKey("player_code", "public.\"ddrPlayerDetails\"(code)", "RESTRICT", "RESTRICT").
		GetErrors()
	if errs != nil && len(errs) > 0 {
		glog.Warningln("fk creation for ddr_models.WorkoutData contained errors:")
		for _, err := range errs {
			glog.Warningf("\t%s\n", err.Error())
		}
	}

	errs = migrator.db.Model(&ddr_models.SongStatistics{}).
		AddForeignKey("song_id,mode,difficulty", "public.\"ddrSongDifficulties\"(song_id,mode,difficulty)", "RESTRICT", "RESTRICT").
		GetErrors()
	if errs != nil && len(errs) > 0 {
		glog.Warningln("fk creation for ddr_models.SongStatistics contained errors:")
		for _, err := range errs {
			glog.Warningf("\t%s\n", err.Error())
		}
	}

	errs = migrator.db.Model(&ddr_models.SongStatistics{}).
		AddForeignKey("player_code", "public.\"ddrPlayerDetails\"(code)", "RESTRICT", "RESTRICT").
		GetErrors()
	if errs != nil && len(errs) > 0 {
		glog.Warningln("fk creation for ddr_models.SongStatistics contained errors:")
		for _, err := range errs {
			glog.Warningf("\t%s\n", err.Error())
		}
	}

	errs = migrator.db.Model(&ddr_models.Score{}).
		AddForeignKey("song_id,mode,difficulty", "public.\"ddrSongDifficulties\"(song_id,mode,difficulty)", "RESTRICT", "RESTRICT").
		GetErrors()
	if errs != nil && len(errs) > 0 {
		glog.Warningln("fk creation for ddr_models.Score contained errors:")
		for _, err := range errs {
			glog.Warningf("\t%s\n", err.Error())
		}
	}

	errs = migrator.db.Model(&ddr_models.Score{}).
		AddForeignKey("player_code", "public.\"ddrPlayerDetails\"(code)", "RESTRICT", "RESTRICT").
		GetErrors()
	if errs != nil && len(errs) > 0 {
		glog.Warningln("fk creation for ddr_models.Score contained errors:")
		for _, err := range errs {
			glog.Warningf("\t%s\n", err.Error())
		}
	}
}