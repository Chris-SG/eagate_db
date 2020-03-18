package user_db

import (
	"github.com/chris-sg/eagate_models/user_models"
	"github.com/golang/glog"
	"github.com/jinzhu/gorm"
	"net/http"
	"strings"
	"time"
)

func SetCookieForUser(db *gorm.DB, userId string, cookie *http.Cookie) {
	glog.Infof("SetCookieForUser for user id %s\n", userId)
	userId = strings.ToLower(userId)
	eaGateUser := RetrieveUserById(db, userId)
	if eaGateUser == nil {
		eaGateUser = &user_models.User{Name: userId}
	}
	eaGateUser.Name = strings.ToLower(eaGateUser.Name)
	eaGateUser.Cookie = cookie.String()
	eaGateUser.Expiration = cookie.Expires.UnixNano() / 1000

	err := db.Save(eaGateUser).Error
	if err != nil {
		glog.Errorf("SetCookieForUser failed: %s\n", err.Error())
	}
}

func RetrieveUserById(db *gorm.DB, userId string) *user_models.User {
	glog.Infof("RetrieveUserById for user id %s\n", userId)
	userId = strings.ToLower(userId)
	var eaGateUser user_models.User
	err := db.Model(&user_models.User{}).Where("account_name = ?", userId).First(&eaGateUser).Error
	if err != nil {
		glog.Errorf("RetrieveUserById failed: %s\n", err.Error())
		return nil
	}
	return &eaGateUser
}

func RetrieveUserByWebId(db *gorm.DB, webUserId string) []user_models.User {
	glog.Infof("RetrieveUserByWebId for web user id %s\n", webUserId)
	webUserId = strings.ToLower(webUserId)
	users := make([]user_models.User, 0)
	err := db.Model(&user_models.User{}).Where("web_user = ?", webUserId).Scan(&users).Error
	if err != nil {
		glog.Errorf("RetrieveUserByWebId failed: %s\n", err.Error())
	}
	return users
}

func RetrieveUserCookieById(db *gorm.DB, userId string) *string {
	glog.Infof("RetrieveUserCookieById for user id %s\n", userId)
	userId = strings.ToLower(userId)
	eaGateUser := RetrieveUserById(db, userId)
	if eaGateUser == nil {
		glog.Warningf("RetrieveUserCookieById: eaGateUser for user id %s was nil\n", userId)
		return nil
	}
	timeNow := time.Now().UnixNano() / 1000
	if len(eaGateUser.Cookie) == 0 || eaGateUser.Expiration < timeNow {
		glog.Warningf("RetrieveUserCookieById: cookie for user id %s was not found or expired\n", userId)
		return nil
	}
	glog.Infof("RetrieveUserCookieById: retrieved cookie for user id %d\n", userId)
	return &eaGateUser.Cookie
}

func SetWebUserForUser(db *gorm.DB, userId string, webId string) {
	glog.Infof("SetWebUserForUser: user id %s, web id %s\n", userId, webId)
	userId = strings.ToLower(userId)
	webId = strings.ToLower(webId)
	eaGateUser := RetrieveUserById(db, userId)
	if eaGateUser == nil {
		glog.Warningf("SetWebUserForUser: eagateuser not found for user id %s\n", userId)
		return
	}
	eaGateUser.WebUser = webId
	err := db.Save(eaGateUser).Error
	if err != nil {
		glog.Errorf("SetWebUserForUser failed: %s\n", err.Error())
	}
}