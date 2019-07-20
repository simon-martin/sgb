package main

import (
	"cloud.google.com/go/logging"
	"cloud.google.com/go/storage"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/golang/glog"
	"github.com/spf13/viper"
	"google.golang.org/api/iterator"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
)

/*
TODOs - rough priority order
- Write aggregated results every n seconds for terminal output only
- More sophisticated human readable bytes
- multi-thread walker part, particularly crc checking
- What if a quadrillion cloud files? sqllite? batching? Eat the round trips?!!
- Allow multiple backup roots directories
- Turn it into a service?
- Pid file more sophisticated e.g. can run if backing up different places
*/

// Logging approach is use glog while we don't have Stackdriver logger open, other wise use log() to use both
var sdlg logging.Logger

// Global map of what we have in storage
var storageMap = make(map[string]storage.ObjectAttrs)

// Set up our channels for workers
var uploadJobs = make(chan string, 100)
var deleteJobs = make(chan string, 100)

// Used to report a non-fatal error during upload so we can tell the user to check the logs
var reportError = false

// Used for some aggregate stats
var stats struct {
	filesScanned  int64
	filesUploaded int64
	filesDeleted  int64
	bytesUploaded int64
}

func main() {
	// Set all our flags
	dryRun := flag.Bool("dry-run", false, "True if you want to make no changes")
	flag.Bool("force", false, "Override the egregious deletions check")
	if err := flag.Set("logtostderr", "true"); err != nil {
		// Can this even fail?
		glog.Fatalf("%s", err)
	}
	flag.Parse()

	// Viper integrates with flags, but can't be bothered to work it out, or maybe flags have global get I missed?
	viper.Set("dry-run", dryRun)

	// Ensure no other running process
	if err := writePidFile("/tmp/sgbpid"); err != nil {
		glog.Fatalf("%s", err)
	}

	// Read in config
	readConfig()

	//Sanity check before we go off to the internet and whatnot
	if _, err := os.Stat(viper.GetString("backup_root_dir")); os.IsNotExist(err) {
		glog.Fatalf("Could not access backup dir: %s", err)
	}

	// Set up our google authentication
	auth := viper.GetString("google_auth")
	if auth == "" {
		glog.Fatalf("No google_auth Path provided")
	}
	if err := os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", auth); err != nil {
		// Can this even fail?
		glog.Fatalf("%s", err)
	}

	/*
		This is frustrating, project_id is in the authentication JSON file, why does the logger not read it from there
		like the storage client does? I tried to find a way to read this data from gcloud libraries but gave up. So data
		has to be duplicated into main config for now :(
	*/
	logClient, err := logging.NewClient(context.Background(), viper.GetString("project_id"))
	if err != nil {
		glog.Fatalf("Could not initialise logging: %s", err)
	}
	sdlg = *logClient.Logger("simple-google-backup")

	// All set up, let's go
	run()
	log(logging.Info, "Files Scanned: %d, Files Uploaded: %d, Megabytes Uploaded: %d, Files Deleted %d",
		stats.filesScanned, stats.filesUploaded, stats.bytesUploaded/1024/1024, stats.filesDeleted)

	// This flushes all waiting messages
	err = logClient.Close()
	if err != nil {
		glog.Warningf("Error Closing logging, some events may not be logged: %s", err)
	}

	// Just so we have a non-zero exit status
	if reportError {
		glog.Fatalf("Run completed with errors!")
	}
	glog.Infof("Run complete!")
}

func readConfig() {
	// Reads the config file or fails fatally
	var configPath = flag.String("config_path", "$HOME/.config", "Path to sgb_config")
	viper.SetConfigName("sgb_config")
	viper.AddConfigPath(*configPath)
	viper.AddConfigPath(".")

	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		glog.Fatalf("Fatal error reading config file: %s", err)
	}

	glog.Infof("Using %s", viper.ConfigFileUsed())
}

func run() {
	log(logging.Info, "Started run for: %s", viper.GetString("backup_root_dir"))

	// Fill our cache, tests our credentials, done in main as it's the last fatal error before we start properly
	err := fillStorageMap()
	if err != nil {
		log(logging.Error, "Failed to fill storage map: %s", err)
		return
	}

	// Start the upload and delete workers
	// I'm not sure to handle clients failing to initialise, I guess the channels close and we break adding to queue?
	var wg = sync.WaitGroup{}
	for i := 1; i <= viper.GetInt("upload_workers"); i++ {
		go uploadWorker(&wg, uploadJobs)
	}
	for i := 1; i <= viper.GetInt("delete_workers"); i++ {
		go deleteWorker(&wg, deleteJobs)
	}

	// Run the backup
	backup()

	// Wait for it all to finish
	wg.Wait()
}

func getStorageClient() (*storage.Client, error) {
	// Return a client connection or fail fatally
	client, err := storage.NewClient(context.TODO())
	if err != nil {
		return nil, err
	}
	return client, nil
}

func fillStorageMap() error {
	// Caches all files in memory. Brute force but fast, won't scale :(
	storageClient, err := getStorageClient()
	if err != nil {
		return err
	}

	bucket := storageClient.Bucket(viper.GetString("bucket_name"))
	objectsIter := bucket.Objects(context.TODO(), nil)
	for {
		attrs, err := objectsIter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		storageMap[attrs.Name] = *attrs
	}

	err1 := storageClient.Close()
	if err1 != nil { // Handle errors reading the config file
		log(logging.Warning, "Error closing Storage Client: %s", err)
	}
	return nil
}

func uploadWorker(wg *sync.WaitGroup, jobs <-chan string) {
	wg.Add(1)
	defer wg.Done()

	storageClient, err := getStorageClient()
	if err != nil {
		log(logging.Error, "Upload worker could not initialise client: %s", err)
		return
	}

	for relPath := range jobs {
		absPath := filepath.Join(viper.GetString("backup_root_dir"), relPath)
		log(logging.Info, "Uploading: %s", absPath)

		file, err := os.Open(absPath)
		if err != nil {
			log(logging.Error, "Failed to open file for upload: %s", err)
			return
		}

		if viper.GetBool("dry-run") {
			log(logging.Warning, "dry-run - Would have uploaded: %s", relPath)
		} else {
			writer := storageClient.Bucket(viper.GetString("bucket_name")).Object(relPath).NewWriter(context.TODO())
			written, err := io.Copy(writer, file)
			if err != nil {
				// Question - do we need to clean up half uploaded files? I assume Google does this...
				log(logging.Error, "Failed during upload of %s: %s", absPath, err)
			} else if err := writer.Close(); err != nil {
				// Question - do we need to clean up half uploaded files? I assume Google does this...
				log(logging.Error, "Failed to finalise upload of %s: %s", absPath, err)
			} else {
				log(logging.Info, "Uploaded %s: %d MB", absPath, written)
				atomic.AddInt64(&stats.filesUploaded, 1)
				atomic.AddInt64(&stats.bytesUploaded, written)
			}
		}
		if err = file.Close(); err != nil {
			log(logging.Warning, "Failed to close file after upload: %s", err)
		}
	}
}

func deleteWorker(wg *sync.WaitGroup, jobs <-chan string) {
	wg.Add(1)
	defer wg.Done()

	storageClient, err := getStorageClient()
	if err != nil {
		log(logging.Error, "Delete worker could not initialise client: %s", err)
		return
	}

	for relPath := range jobs {
		if viper.GetBool("dry-run") {
			log(logging.Warning, "dry-run - Would have deleted: %s", relPath)
		} else {
			o := storageClient.Bucket(viper.GetString("bucket_name")).Object(relPath)
			if err := o.Delete(context.TODO()); err != nil {
				log(logging.Error, "Failed to delete: %s", err)
				return
			}
			log(logging.Info, "Deleted %s", relPath)
			atomic.AddInt64(&stats.filesDeleted, 1)
		}
	}
}

func backup() {
	// At this stage all errors handled on a per-file-basis
	_ = filepath.Walk(viper.GetString("backup_root_dir"), checkPath)
	close(uploadJobs)

	// At this point anything left in map is not on local disk and should be removed
	if len(storageMap) < 100 || viper.GetBool("force") {
		log(logging.Debug, "Left with %d files in storage not seen on local", len(storageMap))
		for relPath := range storageMap {
			deleteJobs <- relPath
		}
	} else {
		log(logging.Warning,
			"Found an egregious number of files to delete. Not deleting %d files, use -force to do so",
			len(storageMap))
	}
	close(deleteJobs)
}

func checkPath(absPath string, info os.FileInfo, err error) error {
	// Takes a raw path from the walker, passes on dirs, sends files for backup
	log(logging.Debug, "Checking %s", absPath)
	// This is actually single threaded atm, but still...
	atomic.AddInt64(&stats.filesScanned, 1)

	file, err := os.Stat(absPath)
	if err != nil {
		log(logging.Error, "Failed to stat path: %s", err)
	} else if file.IsDir() == false {
		checkFile(absPath, file)
	}
	return nil
}

func checkFile(absPath string, file os.FileInfo) {
	// Checks to see if a file is new or changed and uploads if it is
	log(logging.Debug, "Searching storage for: %s", absPath)

	// Storage root is take from the last level of the backup_root_dir
	relPath, err := filepath.Rel(viper.GetString("backup_root_dir"), absPath)
	if err != nil {
		// Odd things happening if we get here...
		log(logging.Error, "Failed to get relative path: %s", err)
	}

	// Look to see if this file exists in storage
	attr, ok := storageMap[relPath]
	if ok == false {
		// Exists locally, not in storage
		uploadJobs <- relPath
	} else if attr.Size != file.Size() {
		// It changed, re-upload
		uploadJobs <- relPath
	} else if viper.GetBool("crc_change_check") {
		// Whats the cost? It's real, 30% maybe
		// TODO this area is one thread, can queue this up too but it's only the bottle neck with low amount of work
		localCRC, err := getCRC(absPath)
		if err != nil {
			log(logging.Error, "Failed to get local crc path: %s", err)
		}

		if localCRC != attr.CRC32C {
			// It changed, re-upload
			uploadJobs <- relPath
		}
	}

	// Either we uploaded it or it was already there and not changed, either way we're in sync
	// Removed from cache, at the end, anything left in cache is not on the local disk in theory
	// Even if we fail above we remove here, it's not a candidate for deletion, needs investigation or will be uploaded
	// next time
	delete(storageMap, relPath)
}

func getCRC(absPath string) (uint32, error) {
	//Returns CRC of the file
	file, err := os.Open(absPath)
	if err != nil {
		// Hard to imagine how we get here unless FS goes away?
		return 0, err
	}
	defer file.Close()

	// Some magic to make it do CRC32C, which is what Google do
	table := crc32.MakeTable(crc32.Castagnoli)
	hashFunc := crc32.New(table)

	_, err1 := io.Copy(hashFunc, file)
	if err1 != nil {
		return 0, err1
	}

	return hashFunc.Sum32(), nil
}

// Stolen, shamelessly, from https://gist.github.com/davidnewhall/3627895a9fc8fa0affbd747183abca39
// Write a pid file, but first make sure it doesn't exist with a running pid.
func writePidFile(pidFile string) error {
	// Read in the pid file as a slice of bytes.
	if piddata, err := ioutil.ReadFile(pidFile); err == nil {
		// Convert the file contents to an integer.
		if pid, err := strconv.Atoi(string(piddata)); err == nil {
			// Look for the pid in the process list.
			if process, err := os.FindProcess(pid); err == nil {
				// Send the process a signal zero kill.
				if err := process.Signal(syscall.Signal(0)); err == nil {
					// We only get an error if the pid isn't running, or it's not ours.
					return fmt.Errorf("pid already running: %d", pid)
				}
			}
		}
	}
	// If we get here, then the pidfile didn't exist,
	// or the pid in it doesn't belong to the user running this app.
	return ioutil.WriteFile(pidFile, []byte(fmt.Sprintf("%d", os.Getpid())), 0664)
}

func log(severity logging.Severity, message string, args ...interface{}) {
	payload := make(map[string]interface{})
	payload["hostname"], _ = os.Hostname()
	payload["pid"] = os.Getpid()
	payload["message"] = fmt.Sprintf(message, args...)
	jsonPayload, _ := json.Marshal(payload)

	// It's not clear to me why RawMessage is needed here, without it I get:
	// "logging: json.Unmarshal: json: cannot unmarshal string into Go value of type map[string]interface {}"
	// But this works fine:
	// json.Unmarshal(jsonPayload, make(map[string]interface {}))
	sdlg.Log(logging.Entry{Payload: json.RawMessage(jsonPayload), Severity: severity})

	switch severity {
	case logging.Error:
		glog.Errorf(message, args...)
		reportError = true
	case logging.Warning:
		glog.Warningf(message, args...)
	case logging.Info:
		glog.Infof(message, args...)
	}
}
