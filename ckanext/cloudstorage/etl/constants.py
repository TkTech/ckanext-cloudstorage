import logging
import configparser

# Configure Logging
log = logging.getLogger('Config')


class ConfigNotFound(Exception):
    """Exception raised for missing configuration.
    Attributes:
        message -- explanation of the error
    """
    def __init__(self, message):
        self.message = message
        super(ConfigNotFound, self).__init__(self.message)


class ConfigurationManager(object):
    """
    Manages loading and accessing configuration settings from a file.

    This class uses a class method to load configuration settings from a specified
    file and provides a method to retrieve individual configuration values.
    """

    _config = None
    _loaded = False

    @classmethod
    def load_config(cls, config_file):
        """
        Load the configuration from a file.

        Args:
            config_file (str): The path to the configuration file.

        Raises:
            ConfigNotFound: If the file is not found.
            Exception: For any other issues encountered during file loading.
        """
        if not cls._loaded:
            cls._config = configparser.ConfigParser()
            try:
                with open(config_file) as f:
                    cls._config.readfp(f)
                cls._loaded = True
                log.info("Configuration loaded successfully from {}".format(config_file))
            except IOError:
                log.error("Unable to find '{}'".format(config_file))
                raise Exception("Unable to find '{}'".format(config_file))
            except Exception as e:
                log.error("Unexpected error while attempting to load '{}': {}".format(config_file, e))
                raise Exception("Unexpected error while attempting to load '{}': {}".format(config_file, e))

    @classmethod
    def get_config_value(cls, section, option, error_msg):
        """
        Retrieve a configuration value from the loaded configuration.

        Args:
            section (str): The section in the configuration file.
            option (str): The option key to retrieve.
            error_msg (str): Error message to display if the option is not found.

        Returns:
            str: The value of the configuration option.

        Raises:
            Exception: If the configuration is not loaded or the option is not found.
        """
        if not cls._loaded:
            raise Exception("Configuration not loaded. Call 'load_config' first.")
        try:
            return cls._config.get(section, option)
        except configparser.NoOptionError:
            log.error(error_msg)
            raise ConfigNotFound(message=error_msg)
