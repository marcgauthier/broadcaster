/*
This package will receive a buffer that normally contains a JSON object and rebroadcast this
buffer at different interval to a UDP:Port destination.  For APP the the UDP address should
be a network address so that information is broadcasted.  This will iliminate the need
for setting a static entry in the ARP table of the APP-RELAY.
*/

package broadcaster

import (
	"errors"
	"github.com/marcgauthier/pronto"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"
)

/* const use to indicate a packet need to be send now, at load time the broadcaster might
   be requested to send data at random interval to avoid overloading the network
   connection and sending the complete content of the database at once.  This const is
   use by other package that are calling the Update function.
*/
const BroadcastNow = 0

// On each LOW And HIGH Network component the MTU Should be set to 8192 or same value as
// the Const.  Because there are no devices between the two network card you should be
// able to set any MTU.  Most packet send by RELAY should be around 150b
const max_udp_packet_size = 8192

/* Channel for creating a list of item that need to be broadcasted, list contains pointer of item
 */
var broadcastChannel chan *item

/* Channel of new incomming update or insert
 */
var updateChannel chan *item

type item struct {
	/* bellow is data received by broadcaster, the broadcaster might send more information
	   but this is what we need everything else is extra and does not need to be sent to
	   couchdb, remember the application-server should only update the status of probes
	   and queries.  Those need to be manually define in the CouchDB by the client application.
	*/
	ID            string
	Buffer        []byte
	nextBroadcast int64 // private - do not broadcast
	broadcastStep int64 // private - do not broadcast
}

// List of Item currently being broadcasted.
var items map[string]*item

// lock to control the list of items
var mutexItems = &sync.RWMutex{}

// pool to reuse the items that goes in the list.
var pool = sync.Pool{
	// New creates an object when the pool has nothing available to return.
	// New must return an interface{} to make it flexible. You have to cast
	// your type after getting it.
	New: func() interface{} {
		// Pools often contain things like *bytes.Buffer, which are
		// temporary and re-usable.
		return &item{}
	},
}

/* get one item from the pool, item will be allocated but field Buffer len will be zero
 */
func getItemFromPool() *item {
	return pool.Get().(*item)
}

/* put back one item in the pool, clear the buffer that will no longer be use
 */
func putItemInPool(i *item) {
	// keep the .Buffer property so it can be reused later.
	pool.Put(i)
}

/* This is the main broadcast function that will at specific intervals broadcast all the devices
   Start the Broadcast function and send all the devices we know to the APP-GUARD at specific
   intervals.  The information is send as a json message by threads that are created by this function.

   Broadcast_Multiplier = default 10
   ChannelSize = default 4096, for both the Update and Broadcast channel
   Default output IP 127.0.0.1
   Default output Port 512

*/
func Start(BroadcasterName string, ToIP string, ToPort, ChannelSize int, workercount int,
	Broadcast_Multiplier, broadcastmaxinterval, broadcastmininterval int64) error {

	// set to default if no IP provided,
	if ToIP == "" {
		ToIP = "127.0.0.1"
	}

	// set to default if port provided is invalid
	if ToPort <= 0 || ToPort > 65535 {
		ToPort = 512
	}

	// set default channel size if size if negative.
	if ChannelSize <= 0 {
		ChannelSize = 4096
	}

	// make sure there are at least 2 workers working.
	if workercount <= 1 {
		workercount = 8
	}

	// set minimum value
	if Broadcast_Multiplier <= 0 {
		Broadcast_Multiplier = 2
	}

	// set minimum value
	if broadcastmaxinterval < 1 {
		broadcastmaxinterval = 1
	}

	// set minimum value
	if broadcastmininterval < 1 {
		broadcastmininterval = 1
	}

	pronto.Info("Starting Broadcaster " + BroadcasterName + " " + ToIP + ":" + strconv.Itoa(ToPort) + " channel size:" + strconv.Itoa(ChannelSize) + " threads:" + strconv.Itoa(workercount) +
		" broadcastMult: " + strconv.FormatInt(Broadcast_Multiplier, 10) + " broadcastMax: " + strconv.FormatInt(broadcastmaxinterval, 10) +
		" broadcastMin: " + strconv.FormatInt(broadcastmininterval, 10))

	/* Allocate memory for holding the buffer items
	 */
	items = make(map[string]*item)

	// channel for sending the ID of packet that need to be broadcasted
	broadcastChannel = make(chan *item, ChannelSize)

	// channel for adding or updating packet buffer's
	updateChannel = make(chan *item, ChannelSize)

	/* create the udp worker that will send the udp packets
	 */
	for i := 0; i < workercount; i++ {
		go broadcastUDPThread(ToIP, ToPort, broadcastmininterval, broadcastmaxinterval, Broadcast_Multiplier)
	}

	/* this function monitor the channel updateChannel, user call the Update function but the
	   Update fonction only put buffer into channel.  processUpdates() will monitor that
	   channel for all updates push into the package.
	*/
	go processUpdates()

	/* monitor all items and flag the one that need to be broadcasted by removing them
	   from the map and inserting them into a Channel for broadcasting.
	*/
	go findItemThatNeedBroadcasting()

	return nil
}

/* This function return a value if it's between limits value of the
   minimum or maximum allows value otherwise return the set limits.
*/
func minMax64(min, max, value int64) int64 {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

/* Insert or Replace an object in the database, each object is broadcasted
   at regular interval.  Buffer will be copy so that the calling function
   can do what they want with the buffer.  This is not the most efficient
   process but is safe.  randomStartMaxSecs would be use if the application
   load a database and is trowing 1000's at the same time.

*/
func Update(Id string, Buffer []byte, randomStartMaxSecs int64) {

	pronto.Info("Receive Update() request for " + Id)

	/* either start broadcast now (0) or use a random number
	 */
	next := time.Now().Unix()
	if randomStartMaxSecs > 0 {
		next += rand.Int63n(randomStartMaxSecs)
	}

	// there is no allocation here if getitemFromPool use an object from the pool
	i := getItemFromPool()
	i.ID = Id
	i.nextBroadcast = next
	i.broadcastStep = 0

	// copy the buffer into the object we got from the pool if the
	// buffer size is not large enough allocate more memory.
	needBYtes := len(Buffer)
	if cap(i.Buffer) < needBYtes {
		i.Buffer = make([]byte, needBYtes)
	}
	copy(i.Buffer, Buffer)

	updateChannel <- i
}

/* Remove a buffer from the broadcasting items and store it back in the pool.
 */
func Delete(Id string) error {

	pronto.Info("Receive Delete() request for " + Id)

	// lock the items map.
	mutexItems.Lock()
	defer mutexItems.Unlock()

	if item, ok := items[Id]; ok {
		// put item back in the pool.
		putItemInPool(item)
		// remove item from the map.
		delete(items, Id)
		return nil
	}

	return errors.New("NotFound")
}

/* process all the update or insert request, this function and channel are used
   because we don't want the Update function to have to wait for a lock before
   returning.  The buffered channel allow the update function to return faster.
*/

func processUpdates() {

	/* loop thru all the updates
	 */
	for item := range updateChannel {

		mutexItems.Lock()
		/* if item is already in the map, put back the item in the pool and
		   overwrite the map to replace the ID so that it point to the new object.
		*/
		if old, found := items[item.ID]; found {
			putItemInPool(old)
		}
		items[item.ID] = item
		mutexItems.Unlock()
	}
}

/* each second go thru the items and find any item need to be broadcasted
 */
func findItemThatNeedBroadcasting() {

	for {

		n := time.Now().Unix()

		/* lock the map iterate items and look for item that need to be broadcasted
		   if item are found ID's must be added to the broadcastChannel
		*/

		mutexItems.Lock()

		/*	go thru all the items remove the one that need broadcasting and store
			them in the broadcast channel.  They will be put back in the map once
			the broadcast is complete.
			To summarize, the delete within a range is safe because the data is technically still there,
			but when it checks the tophash it sees that it can just skip over it and not include it in whatever
			range operation you're performing.

		*/
		for _, item := range items {

			if item.nextBroadcast < n {

				select {
				case broadcastChannel <- item:
					delete(items, item.ID) // remove item from the map it is now in broadcast channel
				default:
				}
			}
		}
		mutexItems.Unlock()

		/* scan probes every 1 second.
		 */
		time.Sleep(time.Second)
	}
}

/* This thread go thru the list of items that need to be send, read the item and convert it  into
   a JSON format string this string is then sent to the APP-GUARD using UDP to an IP or thru
   broadcast packet on a /30 network.
*/

func broadcastUDPThread(broadcastToIP string, broadcastToPort int, broadcastmininterval, broadcastmaxinterval, Broadcast_Multiplier int64) {

	if broadcastToIP == "" || (broadcastToPort >= 65536) {
		pronto.Error("broadcastUDPThread() broadcast IP or Port issue " + broadcastToIP + " " + strconv.Itoa(broadcastToPort))
		return
	}

	/* loop until program end or channel close get the next ID to broadcast
	 */
	for item := range broadcastChannel {

		// no lock is require because the item ptr has been removed from the map.

		/* make sure we actually have something to send; this item length is not empty
		 */
		msgsize := len(item.Buffer)

		if msgsize >= 0 && msgsize <= max_udp_packet_size {

			RemoteAddr, err := net.ResolveUDPAddr("udp", broadcastToIP+":"+strconv.Itoa(int(broadcastToPort)))
			if err != nil {
				pronto.Error(err.Error())
				continue
			}

			/* create the UDP sender.
			 */
			conn, err := net.DialUDP("udp", nil, RemoteAddr)
			if err != nil {
				pronto.Error(err.Error())
				continue
			}

			if conn == nil {
				pronto.Error("conn is nil but no error")
				continue
			}

			/* Write must return a non-nil error if it returns n < len(p). Write must not modify the slice data, even temporarily.
			 */
			if _, err = conn.Write(item.Buffer[:msgsize]); err != nil {
				pronto.Error(err.Error())
				continue
			}

			if conn != nil {
				conn.Close()
			}

			/* now add item back to the map unless a newer item as taken it's place
			 */
			item.broadcastStep = minMax64(broadcastmininterval, broadcastmaxinterval, item.broadcastStep*Broadcast_Multiplier)
			item.nextBroadcast = time.Now().Unix() + item.broadcastStep

			pronto.Info("sent: " + item.ID + " { " + string(item.Buffer) + " } nextBroadcast in " + strconv.FormatInt(item.broadcastStep, 10) + " secs")

			mutexItems.Lock()

			if _, ok := items[item.ID]; !ok {
				// no item in the map insert our item.
				items[item.ID] = item
			} else {
				// newer item in the map, discard our item return to the pool.
				putItemInPool(item)
			}

			mutexItems.Unlock()

		} /* end if msg len is > 0 */

	} // infinite for loop
}
