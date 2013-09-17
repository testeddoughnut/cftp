#!/usr/bin/env python

from prettytable import PrettyTable
import pyrax
import utils

class Cftp(object):

    def __init__(self, delimiter="/"):
        self.delimiter = delimiter
        self.region = None
        self.container = None
        self.prefix = None

    def authenticate(self, username, api_key, region, snet=False,
                    identity_type="rackspace"):
        self.username = username
        self.api_key = api_key
        self.region = region
        public = True if not snet else False
        pyrax.set_setting("identity_type", identity_type)
        pyrax.set_credentials(username, api_key)
        self.cf = pyrax.connect_to_cloudfiles(region, public)

    def get_current_loc(self):
        """Returns a dict containing the delimiter, container, and prefix."""
        return { "region": self.region or None, 
                    "container": self.container or None,
                    "prefix": self.prefix or None,
                    "delimiter": self.delimiter }

    def change_prefix(self, new_prefix):
        joined_path = utils.cf_join(self.delimiter, self.prefix or self.delimiter, new_prefix)
        normalized_path = utils.cf_normpath(self.delimiter, joined_path)
        if normalized_path == self.delimiter:
            self.prefix = None
        elif normalized_path.startswith(self.delimiter):
            # Strip off beginning delimiter if present
            self.prefix = normalized_path[1:]
        else:
            self.prefix = normalized_path

    def change_container(self, new_container):
        """Changes container to given container, assuming it exists. Returns
        true on success, false if the given container doesn't exist.
        """
        if self._is_valid_container(new_container):
            self.container = new_container
            self.prefix = None
            return True
        else:
            return False

    def clear_container(self):
        self.container = None
        self.prefix = None

    def change_region(self, new_region, snet=False):
        """Changes region to given region."""
        self.authenticate(self, self.username, self.api_key, new_region, snet)


    def get_listing(self, location, long_listing=False, human_readable=False,
                    show_header=False):

        pass

    def _parse_loc(self, location):

        pass

    def _is_valid_container(self, container):
        """Checks if given container exists."""
        if container in self.cf.list_containers():
            return True
        else:
            return False

    def _is_object_subdir(self, obj):
        """Checks if given object returned from pyrax is a
        pseudo-subdirectory.
        """
        if getattr(obj, "content_type") == "pseudo/subdir":
            return True
        else:
            return False

    def _is_valid_object(self, container, location):
        """
        check if container is valid
        check if object exists
        check if object type is psuedo subdir

        return true.
        """
        if not self._is_valid_container(container):
            return False
        try:
            obj = self.cf.get_object(container, location)
        except:
            return False
        if self._is_object_subdir(obj):
            return False
        return True
