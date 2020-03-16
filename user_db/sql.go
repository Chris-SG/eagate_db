package user_db

import (
	"github.com/chris-sg/eagate_models/user_models"
	"github.com/jinzhu/gorm"
	"net/http"
	"strings"
	"time"
)

func SetCookieForUser(db *gorm.DB, userId string, cookie *http.Cookie) {
	userId = strings.ToLower(userId)
	eaGateUser := RetrieveUserById(db, userId)
	if eaGateUser == nil {
		eaGateUser = &user_models.User{Name: userId}
	}
	eaGateUser.Name = strings.ToLower(eaGateUser.Name)
	eaGateUser.Cookie = cookie.String()
	eaGateUser.Expiration = cookie.Expires.UnixNano() / 1000
	db.Save(eaGateUser)
}

func RetrieveUserById(db *gorm.DB, userId string) *user_models.User {
	userId = strings.ToLower(userId)
	var eaGateUser user_models.User
	err := db.Model(&user_models.User{}).Where("account_name = ?", userId).First(&eaGateUser).Error
	if err != nil {
		return nil
	}
	return &eaGateUser
}

func RetrieveUserByWebId(db *gorm.DB, webUserId string) []user_models.User {
	webUserId = strings.ToLower(webUserId)
	users := make([]user_models.User, 0)
	db.Model(&user_models.User{}).Where("web_user = ?", webUserId).Scan(&users)
	return users
}

func RetrieveUserCookieById(db *gorm.DB, userId string) *string {
	userId = strings.ToLower(userId)
	eaGateUser := RetrieveUserById(db, userId)
	if eaGateUser == nil {
		return nil
	}
	timeNow := time.Now().UnixNano() / 1000
	if len(eaGateUser.Cookie) == 0 || eaGateUser.Expiration < timeNow {
		return nil
	}
	return &eaGateUser.Cookie
}

func SetWebUserForUser(db *gorm.DB, userId string, webId string) {
	userId = strings.ToLower(userId)
	webId = strings.ToLower(webId)
	eaGateUser := RetrieveUserById(db, userId)
	if eaGateUser == nil {
		return
	}
	eaGateUser.WebUser = webId
	db.Save(eaGateUser)
}