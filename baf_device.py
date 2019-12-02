import sys
from linkkit import linkkit
import traceback
import inspect
import time
import logging
import time
import pandas as pd
import numpy as np

# decide which device to activate
device_info = pd.read_excel('data/baf_device.xlsx')
index = int(sys.argv[1])
config = device_info.iloc[index, :]


class CustomerThing(object):
    def __init__(self, product_key, device_name, device_secret, model_path, host_name='cn-shanghai'):
        self.__linkkit = linkkit.LinkKit(
            host_name=host_name,
            product_key=product_key,
            device_name=device_name,
            device_secret=device_secret)
        self.__linkkit.enable_logger(logging.DEBUG)
        self.__linkkit.on_device_dynamic_register = self.on_device_dynamic_register
        self.__linkkit.on_connect = self.on_connect
        self.__linkkit.on_disconnect = self.on_disconnect
        self.__linkkit.on_topic_message = self.on_topic_message
        self.__linkkit.on_subscribe_topic = self.on_subscribe_topic
        self.__linkkit.on_unsubscribe_topic = self.on_unsubscribe_topic
        self.__linkkit.on_publish_topic = self.on_publish_topic
        self.__linkkit.on_thing_enable = self.on_thing_enable
        self.__linkkit.on_thing_disable = self.on_thing_disable
        self.__linkkit.on_thing_event_post = self.on_thing_event_post
        self.__linkkit.on_thing_prop_post = self.on_thing_prop_post
        self.__linkkit.on_thing_prop_changed = self.on_thing_prop_changed
        self.__linkkit.on_thing_call_service = self.on_thing_call_service
        self.__linkkit.on_thing_raw_data_post = self.on_thing_raw_data_post
        self.__linkkit.on_thing_raw_data_arrived = self.on_thing_raw_data_arrived
        self.__linkkit.thing_setup(model_path)  # load model
        self.__call_service_request_id = 0
        # define a shadow device in local
        self.__shadow = {}

    def _set_shadow(self, param):
        for key in param:
            self.__shadow[key] = param[key]

    def on_device_dynamic_register(self, rc, value, userdata):
        if rc == 0:
            print("dynamic register device success, value:" + value)
        else:
            print("dynamic register device fail, message:" + value)

    def on_connect(self, session_flag, rc, userdata):
        print("on_connect:%d,rc:%d,userdata:" % (session_flag, rc))

    def on_disconnect(self, rc, userdata):
        print("on_disconnect:rc:%d,userdata:" % rc)

    def on_topic_message(self, topic, payload, qos, userdata):
        print("on_topic_message:" + topic + " payload:" + str(payload) + " qos:" + str(qos))
        pass

    def on_subscribe_topic(self, mid, granted_qos, userdata):
        print("on_subscribe_topic mid:%d, granted_qos:%s" %
              (mid, str(','.join('%s' % it for it in granted_qos))))
        pass

    def on_unsubscribe_topic(self, mid, userdata):
        print("on_unsubscribe_topic mid:%d" % mid)
        pass

    def on_publish_topic(self, mid, userdata):
        print("on_publish_topic mid:%d" % mid)

    def on_thing_prop_changed(self, params, userdata):
        # first judge if it is mode change
        set_mode = set(params.keys()) & set(['mode_auto', 'mode_low_flow', 'mode_debug', 'mode_hand'])
        if set_mode:
            mode = set_mode.pop()
            if params[mode] == 1:
                params['mode_auto'] = 0
            else:
                params['mode_auto'] = 1
        # set new setting in shadow
        self._set_shadow(params)
        self.__linkkit.thing_post_property(self.__shadow)
        print("on_thing_prop_changed params:" + str(params))

    def on_thing_enable(self, userdata):
        print("on_thing_enable")

    def on_thing_disable(self, userdata):
        print("on_thing_disable")

    def on_thing_event_post(self, event, request_id, code, data, message, userdata):
        print("on_thing_event_post event:%s,request id:%s, code:%d, data:%s, message:%s" %
              (event, request_id, code, str(data), message))
        pass

    def on_thing_prop_post(self, request_id, code, data, message, userdata):
        print("on_thing_prop_post request id:%s, code:%d, data:%s message:%s" %
              (request_id, code, str(data), message))

    def on_thing_raw_data_arrived(self, payload, userdata):
        print("on_thing_raw_data_arrived:%s" % str(payload))

    def on_thing_raw_data_post(self, payload, userdata):
        print("on_thing_raw_data_post: %s" % str(payload))

    def on_thing_call_service(self, identifier, request_id, params, userdata):
        print("on_thing_call_service identifier:%s, request id:%s, params:%s" %
              (identifier, request_id, params))
        self.__call_service_request_id = request_id
        pass

    def user_loop(self):
        self.__linkkit.connect_async()
        time.sleep(1)  # wait for connect
        # initial shadow device
        self.__shadow['mode_auto'] = 1
        self.__shadow['mode_debug'] = 0
        self.__shadow['mode_hand'] = 0
        self.__shadow['mode_low_flow'] = 0
        self.__shadow['show_air_presure'] = round(np.random.rand()*20, 1)
        self.__shadow['show_pool0'] = 0
        self.__shadow['set_flow'] = round(np.random.rand()*30, 0)
        self.__shadow['set_water_gate'] = 0
        self.__shadow['set_air_fan'] = 1

        # now define main logic
        while True:
            # this is normal heart beat
            self.__linkkit.thing_post_property(self.__shadow)
            print(self.__shadow)
            time.sleep(120)


if __name__ == '__main__':
    device1 = CustomerThing(config['ProductKey'], config['DeviceName'],
                            config['DeviceSecret'], 'baf_model.json')
    device1.user_loop()

