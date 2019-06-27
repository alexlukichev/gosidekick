package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/coreos/etcd/client"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("sidekick")

// Example format string. Everything except the message has a custom color
// which is dependent on the log level. Many fields have a custom output
// formatting too, eg. the time returns the hour down to the milli second.
var format = logging.MustStringFormatter(
	"%{color}%{time:15:04:05.000} %{shortfunc} ▶ %{level:.4s} %{id:03x}%{color:reset} %{message}",
)

type arrayFlags []string

func (v *arrayFlags) String() string {
	return "STRING"
}

func (v *arrayFlags) Set(value string) error {
	*v = append(*v, value)
	return nil
}

var (
	debug    = flag.Bool("v", false, "verbose output")
	interval = flag.Int("i", 15, "refresh interval (sec)")
	keys     arrayFlags
	values   arrayFlags
)

var usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s [options] COMMAND...\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  COMMAND  - shell command to execute\n")
	flag.PrintDefaults()
}

func main() {

	var err error

	flag.Var(&keys, "k", "destination etcd key (miltiple occurences allowed)")
	flag.Var(&values, "p", "destination etcd entry value (multiple occurences allowed, must correspond to -k)")

	flag.Usage = usage
	flag.Parse()

	logging.SetFormatter(format)
	if *debug {
		logging.SetLevel(logging.DEBUG, "sidekick")
	} else {
		logging.SetLevel(logging.INFO, "sidekick")
	}

	if len(keys) == 0 {
		fmt.Printf("No destination keys specified")
		os.Exit(1)
	}

	if len(keys) != len(values) {
		fmt.Printf("Mismatch between keys and values")
		os.Exit(1)
	}

	for i := 0; i < len(keys); i++ {
		log.Debugf("Will be publishing: %s ==> %s", keys[i], values[i])
	}

	var transport client.CancelableTransport

	etcdURL, found := os.LookupEnv("ETCDCTL_ENDPOINT")
	if !found {
		etcdURL, found = os.LookupEnv("ETCDCTL_ENDPOINTS")
		if !found {
			etcdURL = "http://127.0.0.1:2379"
		}
	}

	caFile := os.Getenv("ETCDCTL_CACERT")
	keyFile := os.Getenv("ETCDCTL_KEY_FILE")
	certFile := os.Getenv("ETCDCTL_CERT_FILE")

	if len(certFile) > 0 || len(keyFile) > 0 || len(caFile) > 0 {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			log.Fatal(err)
		}

		caCert, err := ioutil.ReadFile(caFile)
		if err != nil {
			log.Fatal(err)
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      caCertPool,
		}
		tlsConfig.BuildNameToCertificate()
		transport = &http.Transport{TLSClientConfig: tlsConfig}
	} else {
		transport = client.DefaultTransport
	}

	var etcdTimeoutSec int
	etcdTimeoutSecStr := os.Getenv("ETCD_TIMEOUT_SEC")
	if etcdTimeoutSecStr != "" {
		etcdTimeoutSec, err = strconv.Atoi(etcdTimeoutSecStr)
		if err != nil {
			log.Fatal("Invalid timeout value: %v", err)
		}
	} else {
		etcdTimeoutSec = 5
	}

	etcd, err := newEtcdClient(
		strings.Split(etcdURL, ","),
		keys,
		values,
		2*time.Duration(*interval)*time.Second,
		transport,
		time.Duration(etcdTimeoutSec)*time.Second)
	if err != nil {
		log.Errorf("Cannot connect to etcd at %s: %s", etcdURL, err.Error())
		os.Exit(1)
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	go func() {
		s := <-sc
		ssig := s.(syscall.Signal)
		log.Errorf("Signal received: %s", ssig.String())
		etcd.cleanup()
		os.Exit(128 + int(ssig))
	}()

	if flag.NArg() < 1 {
		usage()
		os.Exit(2)
	}

	cmd := flag.Args()[0]
	args := flag.Args()[1:]

	proc := newProcess(cmd, args...)
	if err := proc.start(); err != nil {
		log.Errorf("Error starting %s: %s", cmd, err.Error())
		os.Exit(1)
	}

	if err := etcd.publish(); err != nil {
		log.Errorf("Cannot publish to etcd: %s", err.Error())
	}

OUTER_LOOP:
	for {
		select {
		case procRes := <-proc.exit:
			// process died
			log.Infof("Process %s terminated with exit code %d", cmd, procRes)
			break OUTER_LOOP
		case <-time.After(time.Duration(*interval) * time.Second):
			if err := etcd.publish(); err != nil {
				log.Errorf("Cannot publish to etcd: %s", err.Error())
			}
		}
	}

	etcd.cleanup()

}

type process struct {
	cmd  *exec.Cmd
	exit chan int
}

func newProcess(cmd string, args ...string) *process {
	_cmd := exec.Command(cmd, args...)
	_cmd.Stdout = os.Stdout
	_cmd.Stderr = os.Stderr
	return &process{
		cmd:  _cmd,
		exit: make(chan int, 1),
	}
}

func (p *process) start() error {
	if err := p.cmd.Start(); err != nil {
		return err
	}

	go func() {
		err := p.cmd.Wait()
		if err != nil {
			switch _err := err.(type) {
			case *exec.ExitError:
				w, _ := _err.Sys().(syscall.WaitStatus)
				p.exit <- w.ExitStatus()
			default:
				p.exit <- 128
			}
		} else {
			p.exit <- 0
		}
	}()

	return nil
}

func (p *process) kill() error {
	return p.cmd.Process.Kill()
}

func (p *process) signal(sig os.Signal) (chan int, error) {
	if err := p.cmd.Process.Signal(sig); err != nil {
		return nil, err
	}

	return p.exit, nil
}

type etcdClient struct {
	kapi    client.KeysAPI
	keys    []string
	values  []string
	ttl     time.Duration
	timeout time.Duration
}

func newEtcdClient(endpoints []string, keys []string, values []string, ttl time.Duration, transport client.CancelableTransport, timeout time.Duration) (*etcdClient, error) {
	cfg := client.Config{
		Endpoints: endpoints,
		Transport: transport,
		// set timeout per request to fail fast when the target endpoint is unavailable
		HeaderTimeoutPerRequest: 5 * time.Second,
	}
	c, err := client.New(cfg)
	if err != nil {
		return nil, err
	}

	return &etcdClient{
		kapi:    client.NewKeysAPI(c),
		keys:    keys,
		values:  values,
		ttl:     ttl,
		timeout: timeout,
	}, nil
}

func (etcd *etcdClient) cleanupKey(key string) error {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(5*time.Second))
	defer cancel()
	_, err := etcd.kapi.Delete(ctx, key, &client.DeleteOptions{})
	return err
}

func (etcd *etcdClient) cleanup() error {
	for i := 0; i < len(etcd.keys); i++ {
		key := etcd.keys[i]
		err := etcd.cleanupKey(key)
		if err != nil {
			return err
		}
	}
	return nil
}

func (etcd *etcdClient) publishKey(key string, value string) error {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(etcd.timeout*time.Second))
	defer cancel()

	_, err := etcd.kapi.Set(ctx, key, value, &client.SetOptions{
		TTL: etcd.ttl,
	})
	return err
}

func (etcd *etcdClient) publish() error {
	for i := 0; i < len(etcd.keys); i++ {
		key := etcd.keys[i]
		value := etcd.values[i]

		err := etcd.publishKey(key, value)

		if err != nil {
			return err
		}
	}
	return nil
}
