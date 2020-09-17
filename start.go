package main

import (
	"fmt"
    "log"
    "net/http"
    "database/sql"
    _ "github.com/go-sql-driver/mysql"
    "encoding/json"
)

type User struct {
    Username 	string
    Password  	string
}

type CreateQueueForm struct {
	Username 	string
	Password 	string
	Name 		string
}

type ProduceMessageForm struct {
	Username 	string
	Password 	string
	QueueName	string
	Message 	string
}

type DeleteQueueForm struct {
	Username 	string
	Password 	string
	QueueName	string
}

var authenticationFlag bool

func setAuthHandler(w http.ResponseWriter, r *http.Request){
	w.Header().Set("Content-Type", "application/json")
    if r.Method != "POST" {
    	w.WriteHeader(http.StatusNotFound)
    	w.Write([]byte(`{"message": "must be post"}`))
        return
    }

    var p User

    err := json.NewDecoder(r.Body).Decode(&p)
    if err != nil {
        http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }

    err = addUser(p)
    if err != nil {
    	http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }


    w.WriteHeader(http.StatusAccepted)
    // w.Write(resultJson)
}

func configureAuthHandler(w http.ResponseWriter, r *http.Request){
	w.Header().Set("Content-Type", "application/json")
    if r.Method != "POST" {
    	w.WriteHeader(http.StatusNotFound)
    	w.Write([]byte(`{"message": "must be post"}`))
        return
    }

 	var flag bool

    err := json.NewDecoder(r.Body).Decode(&flag)
    if err != nil {
        http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }

    authenticationFlag = flag


    w.WriteHeader(http.StatusAccepted)
}

func addUser(user User) error {
	db := dbConn()
	var name string
	err := db.QueryRow("select username from users where username = ?", user.Username).Scan(&name)
	if err == sql.ErrNoRows{
		insForm, err := db.Prepare("INSERT INTO users(username, password) VALUES(?,?)")
        if err != nil {
            return err
        }
        insForm.Exec(user.Username, user.Password)
	} else if err != nil{
		return err
	} else {
		insForm, err := db.Prepare("UPDATE users SET password=? WHERE username=?")
        if err != nil {
            panic(err.Error())
        }
        insForm.Exec(user.Password, user.Username)
	}
	return nil
}

func createQueueHandler(w http.ResponseWriter, r *http.Request){
	if r.Method != "POST" {
    	w.WriteHeader(http.StatusNotFound)
    	w.Write([]byte(`{"message": "must be post"}`))
        return
    }

    var f CreateQueueForm

    err := json.NewDecoder(r.Body).Decode(&f)
    if err != nil {
        http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }

    if authenticationFlag {
    	if !authenticate(f.Username, f.Password){
    		http.Error(w, err.Error(), http.StatusBadRequest)
    		return
    	}
    }

    err = createQueue(f.Name)
    if err != nil {
    	http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }


    w.WriteHeader(http.StatusAccepted)
}

func produceMessageHandler(w http.ResponseWriter, r *http.Request){
	if r.Method != "POST" {
    	w.WriteHeader(http.StatusNotFound)
    	w.Write([]byte(`{"message": "must be post"}`))
        return
    }

    var f ProduceMessageForm

    err := json.NewDecoder(r.Body).Decode(&f)
    if err != nil {
        http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }

    if authenticationFlag {
    	if !authenticate(f.Username, f.Password){
    		http.Error(w, err.Error(), http.StatusBadRequest)
    		return
    	}
    }

    err = produceMessage(f.QueueName, f.Message)
    if err != nil {
    	http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }


    w.WriteHeader(http.StatusAccepted)	
}

func deleteQueueHandler(w http.ResponseWriter, r *http.Request){
	if r.Method != "DELETE" {
    	w.WriteHeader(http.StatusNotFound)
    	w.Write([]byte(`{"message": "must be delete"}`))
        return
    }

    var f DeleteQueueForm

    err := json.NewDecoder(r.Body).Decode(&f)
    if err != nil {
        http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }

    if authenticationFlag {
    	if !authenticate(f.Username, f.Password){
    		http.Error(w, err.Error(), http.StatusBadRequest)
    		return
    	}
    }

    err = deleteQueue(f.QueueName)
    if err != nil {
    	http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }


    w.WriteHeader(http.StatusAccepted)	
}

func consumeMessageHandler(w http.ResponseWriter, r *http.Request){
	if r.Method != "GET" {
    	w.WriteHeader(http.StatusNotFound)
    	w.Write([]byte(`{"message": "must be get"}`))
        return
    }

    username := r.URL.Query()["username"]
    password := r.URL.Query()["password"]
    queue := r.URL.Query()["queue"]

    if authenticationFlag {
    	if !authenticate(username[0], password[0]){
    		http.Error(w, "err.Error()", http.StatusBadRequest)
    		return
    	}
    }

    result, err := consumeMessage(queue[0])
    if err != nil {
    	http.Error(w, "err.Error()", http.StatusBadRequest)
        return
    }


    resultJson, err := json.Marshal(result)
    if err != nil{
		log.Fatal(err)
	}

    w.WriteHeader(http.StatusOK)
    w.Write(resultJson)
}

func consumeMessage(queue string) (string, error) {
	db := dbConn()
	var queue_id int
	err := db.QueryRow("select id from queue where name = ?", queue).Scan(&queue_id)
	if err != nil{
		return "", err
	}
	var msg string
	var id_message string
	err = db.QueryRow("select id, message from message where queue_id = ? ORDER BY ID LIMIT 1", queue_id).Scan(&id_message, msg)
	if err != nil{
		return "", err
	} 
	insForm, err := db.Prepare("DELETE FROM message WHERE id=?")
    if err != nil {
        return "", err
    }
    insForm.Exec(id_message)
	return msg, nil 
}

func authenticate(username, password string) bool {
	db := dbConn()
	var name string
	err := db.QueryRow("select username from users where username = ? AND password =", username, password).Scan(&name)
	if err != nil{
		return false
	} else {
		return true
	}
}

func createQueue(name string) error {
	db := dbConn()
	err := db.QueryRow("select name from queues where name = ?", name).Scan(&name)
	if err == sql.ErrNoRows{
		insForm, err := db.Prepare("INSERT INTO queues(name) VALUES(?)")
        if err != nil {
            return err
        }
        insForm.Exec(name)
	} else if err != nil{
		return err
	} else {
		return nil
	}
	return nil
}

func produceMessage(queue, message string) error {
	db := dbConn()
	var queue_id int
	err := db.QueryRow("select id from queue where name = ?", queue).Scan(&queue_id)
	if err != nil{
		return err
	} 
	insForm, err := db.Prepare("INSERT INTO messages(queue_id, message) VALUES(?,?)")
    if err != nil {
        return err
    }
    insForm.Exec(queue_id, message)
	return nil 
}

func deleteQueue(queue string) error{
	db := dbConn()
    var queue_id int
	err := db.QueryRow("select id from users where name = ?", queue).Scan(&queue_id)
    delForm, err := db.Prepare("DELETE FROM message WHERE queue_id=?")
    if err != nil {
        return err
    }
    delForm.Exec(queue_id)
    delForm, err = db.Prepare("DELETE FROM queue WHERE id=?")
    if err != nil {
       return err
    }
    delForm.Exec(queue_id)
    return nil
}

func main() {

	fmt.Println("ready")

    http.HandleFunc("/set-auth", setAuthHandler)
    http.HandleFunc("/configure-auth", configureAuthHandler)
    http.HandleFunc("/create-queue", createQueueHandler)
    http.HandleFunc("/produce-message", produceMessageHandler)
    http.HandleFunc("/delete-queue", deleteQueueHandler)
    http.HandleFunc("/consume-message", consumeMessageHandler)
    log.Fatal(http.ListenAndServe(":8080", nil))
}

func dbConn() (db *sql.DB) {
    dbDriver := "mysql"
    dbUser := "root"
    dbPass := "Sukmadjarna1!"
    dbName := "bluebird_database"
    db, err := sql.Open(dbDriver, dbUser+":"+dbPass+"@tcp(localhost:3306)/"+dbName)
    if err != nil {
        panic(err.Error())
    }
    return db
}