module github.com/chris-sg/eagate_db

go 1.13

require github.com/jinzhu/gorm v1.9.12

require (
	github.com/chris-sg/eagate_models v0.0.0
	github.com/t-tiger/gorm-bulk-insert v1.3.0 // indirect
)

replace github.com/chris-sg/eagate_models v0.0.0 => ../eagate_models
