from emonhub_reporter import EmonHubReporter, isIP_v2
from riemann_client.client import Client
from riemann_client.transport import TCPTransport

class EmonHubRiemannReporter(EmonHubReporter):

    def __init__(self, reporterName, queue, **kwargs):
        """Initialize reporter

        """

        # Initialization
        super(EmonHubRiemannReporter, self).__init__(reporterName, queue, **kwargs)
        # add or alter any default settings for this reporter
        self._defaults.update({'batchsize': 100})
        self._cms_settings = {'riemann_address': "0.0.0.0",
                              'riemann_hostname' : "unset",
                              'riemann_port': 5555,
                              'riemann_columns' : [],
                              'riemann_column_names' : []}

        # This line will stop the default values printing to logfile at start-up
        self._settings.update(self._defaults)

        # set an absolute upper limit for number of items to process per post
        self._item_limit = 250



    def warn_setting(self, key, message):
        self._log.warning("Setting " + self.name + " " + key + ": " + message )

    def info_setting(self, key, message):
        self._log.info("Setting " + self.name + " " + key + ": " + message )

    def set_setting_int(self, key, val):
        if isinstance(val, (int, long)) :
            self.info_setting(key, "set")
            self._settings[key] = val
        elif isinstance(val, str) and val.isdigit() :
            self.info_setting(key, "set")
            self._settings[key] = val
        else :
            self.warn_setting(key, "invalid format")
            
    def set_setting_str(self, key, val):
        if isinstance(val, str):
            self.info_setting(key, "set")
            self._settings[key] = val
        else :
            self.warn_setting(key, "invalid format")

    def set_setting_strlist(self, key, val):
        vals = [x.strip() for x in val.split(",")]
        self.info_setting(key, "set to " + str(vals))
        self._settings[key] = vals

    def set_setting_intlist(self, key, val):
        vals = [int(x.strip()) for x in val.split(",")]
        self.info_setting(key, "set to " + str(vals))
        self._settings[key] = vals
        
    def set(self, **kwargs):
        """
        Settings are stored as strings; call int, etc to convert on use
        :param kwargs:
        :return:
        """

        super (EmonHubRiemannReporter, self).set(**kwargs)

        for key, setting in self._cms_settings.iteritems():
            #valid = False
            if not key in kwargs.keys():
                setting = self._cms_settings[key]
            else:
                setting = kwargs[key]
            if key in self._settings and self._settings[key] == setting:
                continue
            elif key == 'riemann_address' :
                if not isIP_v2( setting ) :
                    self.warn_setting(key, "invalid")
                else :
                    self.info_setting(key, "set")
                self._settings[key] = setting
            elif key == 'riemann_hostname' :
                self.set_setting_str(key, setting)
            elif key == 'riemann_port' :
                self.set_setting_int(key, setting)
            elif key == 'riemann_columns' :
                self.set_setting_intlist(key, setting)
            elif key == 'riemann_column_names' :
                self.set_setting_strlist(key, setting)
            else:
                self._log.warning("'%s' is not valid for %s: %s" % (setting, self.name, key))

    def send_event(self, event_name, event_value) :
        with self.riemann as client:
            client.event(host=self._settings['riemann_hostname'],
                         service="emonhub-"+event_name,
                         metric_d=event_value)

    def _process_post(self, databuffer):
        """Send data to server."""
        
        if not hasattr(self, 'riemann'):
            self.riemann = Client(TCPTransport(
                    self._settings['riemann_address'],
                    self._settings['riemann_port']))
        # databuffer is of format:
        # [[timestamp, nodeid, datavalues][timestamp, nodeid, datavalues]]
        # [[1399980731, 10, 150, 250 ...]]

        # logged before apikey added for security
        
        self._log.info(self.name + ": databuffer: " + str(databuffer))
        last_entry = databuffer[-1]
        values = [int(last_entry[x]) for x in self._settings['riemann_columns']]
        for event in zip( self._settings['riemann_column_names'], 
                          values ):
            self.send_event(event[0], event[1])
        return True
