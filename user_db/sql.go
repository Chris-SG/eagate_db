package user_db

import (
	"bufio"
	"fmt"
	"github.com/chris-sg/eagate_models/user_models"
	"github.com/jinzhu/gorm"
	"net/http"
	"strings"
	"time"
)

func SetCookieForUser(db *gorm.DB, userId string, cookie *http.Cookie) {
	eaGateUser := RetrieveUserById(db, userId)
	if eaGateUser == nil {
		eaGateUser = &user_models.User{Name: userId}
	}
	eaGateUser.Cookie = cookie.String()
	eaGateUser.Expiration = cookie.Expires.UnixNano() / 1000
	db.Save(eaGateUser)
}

func RetrieveUserById(db *gorm.DB, userId string) *user_models.User {
	var eaGateUser user_models.User
	err := db.Model(&user_models.User{}).Where("account_name = ?", userId).First(&eaGateUser).Error
	if err != nil {
		return nil
	}
	return &eaGateUser
}

func RetrieveUserByWebId(db *gorm.DB, webUserId string) []user_models.User {
	users := make([]user_models.User, 0)
	db.Model(&user_models.User{}).Where("web_user = ?", webUserId).Scan(&users)
	return users
}

func RetrieveUserCookieById(db *gorm.DB, userId string) *http.Cookie {
	eaGateUser := RetrieveUserById(db, userId)
	if eaGateUser == nil {
		return nil
	}
	timeNow := time.Now().UnixNano() / 1000
	if len(eaGateUser.Cookie) == 0 || eaGateUser.Expiration < timeNow {
		return nil
	}
	rawReq := fmt.Sprintf("GET / HTTP/1.0\r\nCookie: %s\r\n\r\n", eaGateUser.Cookie)
	req, err := http.ReadRequest(bufio.NewReader(strings.NewReader(rawReq)))
	if err != nil {
		return nil
	}
	return req.Cookies()[0]
}

func SetWebUserForUser(db *gorm.DB, userId string, webId string) {
	eaGateUser := RetrieveUserById(db, userId)
	if eaGateUser == nil {
		return
	}
	eaGateUser.WebUser = webId
	db.Save(eaGateUser)
}