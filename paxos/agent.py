import threading
import sys
import json
import socket
import time

PORT_BEGIN = 10000
HEARTBEAT_TIMEOUT = 10 
HEARTBEAT_INTERVAL = 5
MAX_AGENT_CNT = 10 
PROPOSE_TIMEOUT = 20 #the max time not leader, try to propose, the propose timeout
RET_PROPOSE_TIMEOUT =10 #the min time for response to next propose

class PaxosAgent(object):

    def __init__(self, index):
        self._port = PORT_BEGIN + index
        self._index = index
        self._listen = None
        self._cur_ver = 0 #current max version
        self._cur_accept = 0 #current accept leader
        self._cur_leader = 0 #current leader
        self._state = 0#0: not propose, 1: try propose, wait propose_res, 2: try_accept, wait accept_res,
        self._state_lock = threading.Lock() # lock of state
        self._last_propose = 0#last recv propose time
        self._last_heart_beat = 0#last time recv heartbeat from leader
        self._propose_res = {} # tmp propose data
        self._accept_res = {} # tmp accept data

    def get_next_ver(self):
        return (self._cur_ver/MAX_AGENT_CNT + 1)*MAX_AGENT_CNT + self._index

    def tryEndPropose(self):
        if self._state != 0:
            self.log('%d end propose' % self._index)
            self._state = 0
            self._propose_res = {}
            self._accept_res = {}
    def log(self, var):
        f = open("%d.log"%self._index, 'a')
        f.write(time.strftime("[%y-%m-%d:%H:%M:%S]") + var)
        f.write('\n')
        f.close()

    @staticmethod
    def sendCmd(dst, data):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(json.dumps(data), ('127.0.0.1', dst + PORT_BEGIN))

    def handle_propose(self, data):
        #cold time
        #if time.time() - self._last_propose <= RET_PROPOSE_TIMEOUT:
        #    return
        ret = {
            'cmd': 'propose_res',
            'ret': 0
        }

        if self._cur_ver < data['ver']:
            ret['ret'] = 1

        self._last_propse = time.time()
        self.asyncSendCmd(data['from'], ret)

    def handle_accept(self, data):
        ret = {'cmd': 'accept_res', 'ret': 0}
        if self._cur_ver < data['ver']:
            ret['ret'] = 1
            self._cur_ver = data['ver']
            self._cur_accept = data['val']
        self.asyncSendCmd(data['from'], ret)

    def handle_decide(self, data):
        if data['ver'] == self._cur_ver and data['val'] == self._cur_accept:
            self._cur_leader = data['val']
            self._last_heart_beat = time.time()

    def handle_propose_res(self, data):
        if self._state != 1:
            return
        self.log('recv propose_res' + str(data))
        if data['ret'] == 1:
            self._propose_res.update({data['from']: 1})
            if len(self._propose_res) > MAX_AGENT_CNT/2:
                self.broadcast({'cmd': 'accept', 'ver': self.get_next_ver(), 'val': self._index})
                self._state = 2
                #defaut accept self
                self._cur_ver = self.get_next_ver()
                self._cur_accept = self._index
                self.log('send accept ver %d ' % self._cur_ver)
   
    def handle_accept_res(self, data):
        if self._state != 2:
            return
        self.log('recv accept_res' + str(data))
        if data['ret'] == 1:
            self._accept_res.update({data['from']: 1})
            if len(self._accept_res) > MAX_AGENT_CNT/2:
                self.broadcast({'cmd': 'decide', 'ver': self._cur_ver, 'val': self._index})
                self._state = 0
                self._cur_leader = self._index
                self.log('decide ver %d' %  self._cur_ver)

    def handle_heart_beat(self, data):
        dst = data['from']
        self.asyncSendCmd(dst, {'cmd': 'heart_beat_res'})

    def handle_heart_beat_res(self, data):
        self.log('recv heart beat')
        self._last_heart_beat = time.time()

    def handle_request(self, data, addr):
        dict_data = json.loads(data)
        cmd = dict_data['cmd']
        handler_name = 'handle_' + cmd
        if hasattr(self, handler_name):
            handler = getattr(self, handler_name)
            self._state_lock.acquire()
            handler(dict_data)
            self._state_lock.release()

    def startListen(self):
        self._listen = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._listen.bind(('127.0.0.1', self._port))
        while True:
            data, addr = self._listen.recvfrom(1024)
            #print '%d recvfrom ' % self._index, data, addr
            self.handle_request(data, addr)

    def asyncSendCmd(self, dst, data):
        data['from'] = self._index
        t_thread = threading.Thread(target = PaxosAgent.sendCmd, args = (dst, data))
        t_thread.start()

    def broadcast(self, data):
        for dst in range(1, MAX_AGENT_CNT):
            if dst != self._index:
                self.asyncSendCmd(dst, data)

    def heartBeat(self):
        threading.Timer(HEARTBEAT_INTERVAL, self.heartBeat).start()
        try_propose = False
        if self._cur_leader > 0:
            if self._cur_leader != self._index and time.time() - self._last_heart_beat > HEARTBEAT_TIMEOUT and time.time() - self._last_propose > PROPOSE_TIMEOUT:
                try_propose = True
            self.asyncSendCmd(self._cur_leader, {'cmd': 'heart_beat'})
        else:
            try_propose = True
        self._state_lock.acquire() 
        if try_propose and self._state == 0:
            self._state = 1
            threading.Timer(PROPOSE_TIMEOUT, self.tryEndPropose).start()
            self.broadcast({'cmd': 'propose', 'val': self._index, 'ver': self.get_next_ver()})     
            self.log( '%d try propose with ver %d leader %d distime %d' % (self._index, self.get_next_ver(), self._cur_leader, time.time() - self._last_heart_beat ))
        self._state_lock.release()

    def startLogic(self):
        threading.Timer(HEARTBEAT_INTERVAL, self.heartBeat).start()

    def run(self):
        t_listen = threading.Thread(target = self.startListen)
        t_logic = threading.Thread(target = self.startLogic)
        t_listen.start()
        t_logic.start()
        self.log('start %d on port %d' % (self._index, self._port))

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'Error: input illegal\nFormat: %s index' % sys.argv[0]
    else:
        index = int(sys.argv[1])
        agent = PaxosAgent(index)
        agent.run()
