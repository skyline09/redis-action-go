package main

import (
	"github.com/go-redis/redis"
	"log"
	"strconv"
	"strings"
	"time"
)

const (
	ONE_WEEK_IN_SECONDS = 7 * 3600
	VOTE_SCORE          = 432
)

func NewClient(addr string, passwd string, db int) *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: passwd,
		DB:       db,
	})
	return client
}

func voteArticle(client *redis.Client, user string, article string) {
	cutoff := time.Now().Unix()
	timestamp, err := client.ZScore("time:", article).Result()
	if err != nil {
		log.Printf(err.Error())
		return
	}
	if timestamp < float64(cutoff) {
		log.Printf("cannot vote")
		return
	}

	articleID := strings.Split(article, ":")[1]
	intCmd := client.SAdd("voted:"+articleID, user)
	if intCmd.Val() == 1 {
		pipe := client.TxPipeline()
		pipe.ZIncrBy("score:", VOTE_SCORE, article)
		pipe.HIncrBy(article, "votes", 1)
		_, err := pipe.Exec()
		if err != nil {
			log.Printf(err.Error())
		}
	} else {
		log.Printf("cannot vote duplicate")
	}
}

func postArticle(client *redis.Client, user string, title string, link string) string {
	articleID := strconv.Itoa(int(client.Incr("article:").Val()))
	voted := "voted:" + articleID
	client.SAdd(voted, user)
	client.Expire(voted, ONE_WEEK_IN_SECONDS*time.Second)

	now := time.Now().Unix()
	articleKey := "article:" + articleID

	pipe := client.TxPipeline()
	pipe.HMSet(articleKey, map[string]interface{}{
		"title":  title,
		"link":   link,
		"poster": user,
		"time":   now,
		"votes":  1,
	})
	pipe.ZAdd("score:", redis.Z{
		Score:  float64(now + VOTE_SCORE),
		Member: articleKey,
	})
	pipe.ZAdd("time:", redis.Z{
		Score:  float64(now),
		Member: articleKey,
	})
	_, err := pipe.Exec()
	if err != nil {
		log.Printf(err.Error())
		return ""
	}
	return articleID
}

func getArticles(client *redis.Client, page int, order string) []map[string]string {
	const ARTICLES_PER_PAGE = 25
	articles := make([]map[string]string, 0, 0)
	start := (page - 1) * ARTICLES_PER_PAGE
	end := page*ARTICLES_PER_PAGE - 1

	ids, err := client.ZRange(order, int64(start), int64(end)).Result()
	if err != nil {
		log.Printf(err.Error())
		return nil
	}
	for _, id := range ids {
		data := client.HGetAll(id).Val()
		data["id"] = id
		articles = append(articles, data)
	}
	return articles
}

func addGroups(client *redis.Client, articleID string, groups []string) {
	articleKey := "article:" + articleID
	for _, group := range groups {
		client.SAdd("group:"+group, articleKey)
	}
}

func delGroups(client *redis.Client, articleID string, groups []string) {
	articleKey := "article:" + articleID
	for _, group := range groups {
		client.SRem("group:"+group, articleKey)
	}
}

func getGroupArticles(client *redis.Client, group string, page int, order string) []map[string]string {
	key := order + group
	exist, err := client.Exists(key).Result()
	if err != nil || exist == 0 {
		_, err := client.ZInterStore(key, redis.ZStore{},
			"group:"+group, order).Result()
		if err != nil {
			log.Printf(err.Error())
		}
		client.Expire(key, 60*time.Second)
	}
	_, err = client.ZRangeWithScores(key, 0, -1).Result()
	if err != nil {
		log.Printf(err.Error())
	}
	return getArticles(client, page, key)
}

func getAllKeys(client *redis.Client) []string {
	allKeys := make([]string, 0, 0)
	firstScan := true
	var cursor uint64 = 0
	for firstScan || cursor != 0 {
		firstScan = false
		keys, cursor0, err := client.Scan(cursor, "*", 10).Result()
		if err != nil {
			log.Printf(err.Error())
			return nil
		}
		cursor = cursor0
		allKeys = append(allKeys, keys...)
	}
	return allKeys
}

func getDB(client *redis.Client) {
	allKeys := getAllKeys(client)
	articleKeys := make([]string, 0, 0)
	votedKeys := make([]string, 0, 0)
	timeKeys := make([]string, 0, 0)
	scoreKeys := make([]string, 0, 0)

	for _, key := range allKeys {
		if strings.HasPrefix(key, "article:") {
			articleKeys = append(articleKeys, key)
		}

		if strings.HasPrefix(key, "voted:") {
			votedKeys = append(votedKeys, key)
		}

		if strings.HasPrefix(key, "time:") {
			timeKeys = append(timeKeys, key)
		}

		if strings.HasPrefix(key, "score:") {
			scoreKeys = append(scoreKeys, key)
		}
	}

	log.Println("===== article =====")
	for _, key := range articleKeys {
		for k, v := range client.HGetAll(key).Val() {
			log.Printf("[key:%s] %s:%s", key, k, v)
		}
	}

	log.Println("===== score =====")
	for _, key := range scoreKeys {
		res := client.ZRangeWithScores(key, 0, -1).Val()
		for _, z := range res {
			log.Printf("[key:%s] %s %f", key, z.Member, z.Score)
		}
	}

	log.Println("===== time =====")
	for _, key := range timeKeys {
		res := client.ZRangeWithScores(key, 0, -1).Val()
		for _, z := range res {
			log.Printf("[key:%s] %s %f", key, z.Member, z.Score)
		}
	}

	log.Println("===== voted =====")
	for _, key := range votedKeys {
		res := client.SMembers(key).Val()
		for _, mem := range res {
			log.Printf("[key:%s] %s", key, mem)
		}
	}
}

func clearDB(client *redis.Client) {
	client.FlushDb()
}

func main() {
	client := NewClient("127.0.0.1:6379", "", 0)
	clearDB(client)
	a0 := postArticle(client, "sky", "This is sky", "sky.com")
	a1 := postArticle(client, "line", "This is line", "line.com")
	voteArticle(client, "user:user1", "article:"+a0)
	getArticles(client, 0, "score:")

	addGroups(client, a0, []string{"c++", "java"})
	addGroups(client, a1, []string{"c++"})

	log.Println("*********************")
	res := getGroupArticles(client, "c++", 0, "score:")
	for _, r := range res {
		log.Println(r)
	}
	log.Println("*********************")
	res = getGroupArticles(client, "java", 0, "score:")
	for _, r := range res {
		log.Println(r)
	}

	getDB(client)
	clearDB(client)
}
