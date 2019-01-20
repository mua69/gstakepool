package main

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
		_ "github.com/lib/pq"
	"github.com/mua69/gstakepool/log"
	"github.com/mua69/particlrpc"
	"github.com/pebbe/zmq4"
	"io/ioutil"
	"os"
	"time"
	"flag"
)

type Config struct {
	ParticldRpcPort       int    `json:"particld_rpc_port"`
	ParticldRpcHost       string `json:"particld_rpc_host"`
	ParticldDataDir       string `json:"particld_data_dir"`
	ParticldStakingWallet string `json:"particld_staking_wallet"`
	ZmqEndpoint           string `json:"zmq_endpoint"`
	DbUrl                 string `json:"db_url"`
	LogFile               string `json:"log_file"`
}

type TableDef struct {
	name string
	cols string
}

const SatPerPart = 100000000

var gTableDef = []TableDef{ {"stakingratestats", "block_nr int PRIMARY KEY, block_time bigint, nominal_rate numeric, actual_rate numeric"} }

var gConfig Config
var gInitDb bool
var gClearDb bool
var gAvgActualReward = float64(0)
var gDb *sql.DB

func usage() {
	log.Error("Usage: stakingstatd <config.json>")
	flag.PrintDefaults()
	os.Exit(1)
}

func parseCommandLine() {
	flag.BoolVar( &gInitDb, "initdb",false, "initialize database and exit")
	flag.BoolVar( &gClearDb, "cleardb",false, "clears database and exit")
	flag.Parse()
}

func readConfig(filename string) bool {
	data, err := ioutil.ReadFile(filename)

	if err != nil {
		log.Error("Failed to open config file \"%s\": %s\n", filename, err.Error())
		return false
	}

	err = json.Unmarshal(data, &gConfig)
	if err != nil {
		log.Error("Syntax error in config file %s: %v", filename, err)
		return false
	}

	return true
}

func dbConnect() *sql.DB {
	db, err := sql.Open("postgres", gConfig.DbUrl)

	if err != nil {
		log.Error("Cannot connect to data base: %v", err)
		return nil
	}

	err = db.Ping()

	if err != nil {
		log.Error("Cannot connect to data base: %v", err)
		return nil
	}

	return db
}

func dbInit(db *sql.DB) bool {
	for _, d := range gTableDef {
		_, err := db.Exec("create table " + d.name + " (" + d.cols + ");")

		if err != nil {
			log.Error("DB: failed to create table '%s': %v", d.name, err)
			return false
		}
	}

	return true
}

func dbClear(db *sql.DB) bool {
	for _, d := range gTableDef {
		_, err := db.Exec("drop table " + d.name + ";")

		if err != nil {
			log.Error("DB: failed to delete table '%s': %v", d.name, err)
			return false
		}
	}

	return true
}


func dbUpdate(db *sql.DB, blocknr int, blocktime int64, nominalRate, actualRate float64) {
	row := db.QueryRow("SELECT block_nr FROM stakingratestats WHERE block_nr = $1", blocknr)

	var d int

	err := row.Scan(&d)
	if err == sql.ErrNoRows {
		_, err := db.Exec("INSERT INTO stakingratestats (block_nr, block_time, nominal_rate, actual_rate) VALUES ($1, $2, $3, $4)",
			blocknr, blocktime, nominalRate, actualRate)
		if err != nil {
			log.Error("Inserting into db failed: %v", err)
		}
	} else {
		_, err := db.Exec("UPDATE stakingratestats SET block_time = $1, nominal_rate = $2, actual_rate = $3 WHERE block_nr = $4",
		blocktime, nominalRate, actualRate, blocknr)
		if err != nil {
			log.Error("Updating db failed: %v", err)
		}
	}
}


func calcStakingReward(stakeinfo *particlrpc.StakingInfo, blockheader *particlrpc.Block) {
	/*
		var blockReward float64

		blockReward = stakeinfo.Moneysupply * stakeinfo.Percentyearreward * (100 - stakeinfo.Foundationdonationpercent)
		blockReward /= 100 * 100
		blockReward /= BlocksPerYear

		stakingTime := float64(stakeinfo.Expectedtime) / SecondsPerDay

		actualReward := blockReward / stakingTime * 365 * 100 / float64(stakeinfo.Weight) * SatPerPart
	*/

	nominalReward := stakeinfo.Percentyearreward * (100 - stakeinfo.Foundationdonationpercent) / 100

	actualReward := stakeinfo.Moneysupply * stakeinfo.Percentyearreward * (100 - stakeinfo.Foundationdonationpercent)
	actualReward /= 100 * 100
	actualReward /= float64(stakeinfo.Netstakeweight) / SatPerPart
	actualReward *= 100

	if gAvgActualReward != 0 {
		gAvgActualReward = 0.99*gAvgActualReward + 0.01*actualReward
	} else {
		gAvgActualReward = actualReward
	}

	log.Info(0, "Actual avg reward: %.8f", gAvgActualReward)
	dbUpdate(gDb, blockheader.Height, blockheader.Time, nominalReward, actualReward)
}

func getStakingInfo(rpc *particlrpc.ParticlRpc) *particlrpc.StakingInfo {
	var stakeinfo particlrpc.StakingInfo

	err := rpc.CallRpc("getstakinginfo", gConfig.ParticldStakingWallet, nil, &stakeinfo)

	if err == nil {
		return &stakeinfo
	} else {
		log.Error("RPC getstakinginfo failed.")
	}

	return nil
}

func getBlockHeader(rpc *particlrpc.ParticlRpc, hash []byte) *particlrpc.Block {
	var block particlrpc.Block

	args := []interface{}{hex.EncodeToString(hash)}

	err := rpc.CallRpc("getblockheader", gConfig.ParticldStakingWallet, args, &block)

	if err == nil {
		return &block
	} else {
		log.Error("RPC getblockheader failed.")
	}

	return nil
}

func collectStakingStats(rpc *particlrpc.ParticlRpc) {
	zmqContext, err := zmq4.NewContext()
	if err != nil {
		log.Error("zmq context creation failed: %v\n", err)
		return
	}

	zmq, err := zmqContext.NewSocket(zmq4.SUB)
	if err != nil {
		log.Error("zmq socket creation failed: %v\n", err)
		return
	}

	err = zmq.Connect(gConfig.ZmqEndpoint)
	if err != nil {
		log.Error("zmq connect failed: %v\n", err)
		return
	}

	zmq.SetSubscribe("hashblock")

	for {
		msg, err := zmq.RecvMessageBytes(0)
		if err != nil {
			log.Error("zmq receive failed: %v\n", err)
			time.Sleep(10*time.Second)
		} else {
			log.Info(0,"stakingRewardCollector: Processing block: %s\n", hex.EncodeToString(msg[1]))

			blockheader := getBlockHeader(rpc, msg[1])
			stakeinfo := getStakingInfo(rpc)

			if blockheader != nil && stakeinfo != nil {
				calcStakingReward(stakeinfo, blockheader)
			}
		}
	}

}

func main() {
	parseCommandLine()

	if flag.NArg() != 1 {
		usage()
	}

	if !readConfig(flag.Arg(0)) {
		os.Exit(1)
	}

	if gConfig.LogFile != "" {
		log.OpenLogFile(gConfig.LogFile)
		defer log.CloseLogFile()
	}

	gDb = dbConnect()
	if gDb == nil {
		log.Fatal("Failed to connect to database.")
	}

	if gInitDb {
		if dbInit(gDb) {
			return
		} else {
			log.Fatal("Failed to initialize database.")
		}
	}

	if gClearDb {
		if dbClear(gDb) {
			return
		} else {
			log.Fatal("Failed to clear database.")
		}
	}

	rpc := particlrpc.NewParticlRpc()
	rpc.SetDataDirectoy(gConfig.ParticldDataDir)
	rpc.SetRpcHost(gConfig.ParticldRpcHost)
	rpc.SetRpcPort(gConfig.ParticldRpcPort)

	err := rpc.ReadPartRpcCookie()
	if err != nil {
		log.Error("%v", err)
		os.Exit(1)
	}

	log.Info(0, "Starting staking stats collector")
	collectStakingStats(rpc)
}
