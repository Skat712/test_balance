package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gocraft/dbr/v2"
	_ "github.com/lib/pq"
)

var dbConn *dbr.Connection
var cache Cache
var delayedSave DelayedSave

//// КЕШ ПОЛЬЗОВАТЕЛЕЙ /////

type Cache struct {
	Users map[int]*CachedUser
}

type CachedUser struct {
	User     *User
	userLock sync.Mutex
}

func (c *Cache) GetUser(id int) *CachedUser {
	if item, ok := c.Users[id]; ok {
		return item
	}

	item := &CachedUser{
		User: nil,
	}

	c.Users[id] = item

	return item
}

//// ПОЛЬЗОВАТЕЛЬ /////

type User struct {
	ID      int `db:"id"`
	Balance int `db:"balance"`

	ul sync.Mutex
}

func (u *User) DecreaseBalance(amount int) error {
	u.ul.Lock()
	defer u.ul.Unlock()

	if u.Balance == 0 || u.Balance < amount {
		return errors.New("not enough money")
	}

	u.Balance -= amount
	return nil
}

//// ВХОДНЫЕ ПАРАМЕТРЫ РОУТА /////

type BalanceParams struct {
	UserID int `json:"user_id"`
	Amount int `json:"amount"`
}

func (bp *BalanceParams) Validate() error {
	if bp.UserID < 1 {
		return errors.New("invalid user id")
	}

	if bp.Amount < 1 {
		return errors.New("invalid amount")
	}

	return nil
}

///// СОХРАНЕНИЕ ЮЗЕРОВ В ФОНЕ /////

type DelayedSave struct {
	sess     *dbr.Session
	mainChan chan *User
	stopChan chan bool
}

func newDelaySave(sess *dbr.Session) DelayedSave {
	ds := DelayedSave{
		sess:     sess,
		stopChan: make(chan bool),
		mainChan: make(chan *User, 10000),
	}
	ds.Start()
	return ds
}

func (ds *DelayedSave) Close() {
	ds.stopChan <- true
}

func (ds *DelayedSave) Save(user *User) {
	ds.mainChan <- user
}

func (ds *DelayedSave) Start() {
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()

		users := make(map[int]int64)
		log.Println("start bg save")

	loop:
		for {
			select {
			case <-ticker.C:
				// сохраняем юзеров, которых последний раз обновляли более 2 мин назад
				now := time.Now().Unix()
				for userId, updateTime := range users {
					if updateTime < (now - 2*60) {
						log.Printf("Updating user %d", userId)
						user := cache.GetUser(userId).User
						ds.sess.Update("users").Set("balance", user.Balance).Where("id = ?", user.ID).Exec()
						delete(users, userId)
					}
				}

			case user := <-ds.mainChan:
				// сохраняем время когда юзер пришел для обновления
				users[user.ID] = time.Now().Unix()
			case <-ds.stopChan:
				log.Println("stop bg save")
				break loop
			}
		}
	}()
}

// loadUser - Получает пользователя. Сначала смотрит кеш, если нет - идет в БД
func loadUser(sess *dbr.Session, id int) *User {
	item := cache.GetUser(id)
	if item.User != nil {
		return item.User
	}

	item.userLock.Lock()
	defer item.userLock.Unlock()

	res := cache.GetUser(id)
	if res.User != nil {
		return item.User
	}

	user := &User{}
	if rowsCount, _ := sess.Select("*").From("users").Where("id = ?", id).Load(user); rowsCount == 0 {
		return nil
	}

	item.User = user

	return user
}

// BalanceHandler - обработчик роута
func BalanceHandler(w http.ResponseWriter, r *http.Request) {
	var params BalanceParams
	if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
		sendError(w, err, http.StatusBadRequest)
		return
	}

	if err := params.Validate(); err != nil {
		sendError(w, err, http.StatusUnprocessableEntity)
		return
	}

	sess := dbConn.NewSession(nil)
	user := loadUser(sess, params.UserID)
	if user == nil {
		sendError(w, errors.New("user not found"), http.StatusNotFound)
		return
	}

	if err := user.DecreaseBalance(params.Amount); err != nil {
		sendError(w, err, http.StatusBadRequest)
		return
	}

	delayedSave.Save(user)

	sendSuccess(w)
}

// sendError - отправляет сообщение об ошибке клиенту
func sendError(w http.ResponseWriter, err error, status int) {
	response, _ := json.Marshal(map[string]string{
		"error": err.Error(),
	})
	//log.Println(err.Error())
	w.WriteHeader(status)
	w.Write(response)
}

// sendSuccess - отправка успешного ответа клиенту
func sendSuccess(w http.ResponseWriter) {
	response, _ := json.Marshal(map[string]bool{
		"success": true,
	})
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}

// initDB - подключение к базе и создание таблиц
func initDB(psqlInfo string) {
	if env := os.Getenv("PG_CONNECTION_STRING"); len(env) > 0 {
		psqlInfo = env
	}

	db, err := dbr.Open("postgres", psqlInfo, nil)
	if err != nil {
		log.Fatal(err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	dbConn = db
	log.Println("postgres connected!")

	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS public.users (id SERIAL NOT NULL, balance bigint NOT NULL)`); err != nil {
		log.Fatal(err)
	}

	if _, err := db.Exec(`TRUNCATE USERS`); err != nil {
		log.Fatal(err)
	}

	if _, err := db.Exec(`INSERT into users(balance) values (10000)`); err != nil {
		log.Fatal(err)
	}
}

func startHttpServer(port int, wg *sync.WaitGroup) *http.Server {
	srv := &http.Server{Addr: fmt.Sprintf(":%d", port)}

	http.HandleFunc("/user/balance", BalanceHandler)

	go func() {
		defer wg.Done()
		log.Printf("Starting application on port %d", port)
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("ListenAndServe(): %v", err)
		}
	}()

	return srv
}

/////// ТОЧКА ВХОДА /////

func main() {
	// парсим входные параметры
	var port = flag.Int("port", 8080, "listen port")
	var psqlInfo = flag.String("db_connection_string", "host=localhost port=5432 user=skat password=123456 dbname=test_app sslmode=disable", "")
	flag.Parse()

	// инициализация базы
	initDB(*psqlInfo)

	// инициализация кеша
	cache = Cache{
		Users: make(map[int]*CachedUser),
	}

	// запускаем сохранение в фоне
	delayedSave = newDelaySave(dbConn.NewSession(nil))

	wg := &sync.WaitGroup{}
	wg.Add(1)

	srv := startHttpServer(*port, wg)

	// подписываемся на сигналы
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGUSR1, syscall.SIGUSR2)

	// ждем сигнала закрытия
	<-sigchan

	// выключаем все
	fmt.Println()
	log.Println("shutting down...")
	srv.Shutdown(context.Background())
	wg.Wait()
	log.Println("server stopped")
	delayedSave.Close()
	dbConn.Close()
}
