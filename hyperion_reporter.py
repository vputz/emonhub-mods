from emonhub_reporter import EmonHubReporter, isIP_v2
from hyperion_interface import HyperionInterface

class EmonHubHyperionReporter(EmonHubReporter):

    def __init__(self, reporterName, queue, **kwargs):
        """Initialize reporter

        """

        # Initialization
        super(EmonHubHyperionReporter, self).__init__(reporterName, queue, **kwargs)
        # add or alter any default settings for this reporter
        self._defaults.update({'batchsize': 100})
        self._cms_settings = {'hyperion_address': "10.0.0.4",
                              'hyperion_port': 19444,
                              'hyperion_priority' : 1000,
                              'hyperion_column' : 2,
                              'hyperion_threshold': 1000}

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

    def set(self, **kwargs):
        """
        Settings are stored as strings; call int, etc to convert on use
        :param kwargs:
        :return:
        """

        super (EmonHubHyperionReporter, self).set(**kwargs)

        for key, setting in self._cms_settings.iteritems():
            #valid = False
            if not key in kwargs.keys():
                setting = self._cms_settings[key]
            else:
                setting = kwargs[key]
            if key in self._settings and self._settings[key] == setting:
                continue
            elif key == 'hyperion_address' :
                if not isIP_v2( setting ) :
                    self.warn_setting(key, "invalid")
                else :
                    self.info_setting(key, "set")
                self._settings[key] = setting
            elif key == 'hyperion_port' :
                self.set_setting_int(key, setting)
            elif key == 'hyperion_priority' :
                self.set_setting_int(key, setting)
            elif key == 'hyperion_column' :
                self.set_setting_int(key, setting)
            elif key == 'hyperion_threshold' :
                self.set_setting_int(key, setting)
            else:
                self._log.warning("'%s' is not valid for %s: %s" % (setting, self.name, key))

    def send_hot(self):
        try:
            self.hyperion.connect()
            self.hyperion.setColor([255,0,0])
            self.hyperion.close()
            self._log.info(self.name + ": HOT")
            self._log.info( "last result:" + self.hyperion.last_result)
        except Exception as e:
            self._log.error("{0}: exception sending hot: {1}".format(
                    self.name, repr(e)))
        
    def send_cool(self):
        try:
            self.hyperion.connect()
            self.hyperion.clear()
            self.hyperion.close()
            self._log.info(self.name + ": cool")
            self._log.info( "last result:" + self.hyperion.last_result)
        except Exception as e:
            self._log.error("{0}: exception sending cool: {1}".format(
                        self.name, repr(e)))

    def _process_post(self, databuffer):
        """Send data to server."""
        
        if not hasattr(self, 'hyperion'): 
            self.hyperion = HyperionInterface( 
                hyperion_address=self._settings['hyperion_address'],
                hyperion_port=int(self._settings['hyperion_port']),
                default_priority=int(self._settings['hyperion_priority'])
                )
        # databuffer is of format:
        # [[timestamp, nodeid, datavalues][timestamp, nodeid, datavalues]]
        # [[1399980731, 10, 150, 250 ...]]

        # logged before apikey added for security
        
        self._log.info(self.name + ": databuffer: " + str(databuffer))
        last_entry = databuffer[-1]
        if last_entry[int(self._settings['hyperion_column'])] > int(self._settings['hyperion_threshold']):
            self.send_hot()
        else :
            self.send_cool()
        return True
