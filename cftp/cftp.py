#!/usr/bin/env python

from prettytable import PrettyTable
import pyrax
import utils

class Cftp(object):

    def __init__(self, delimiter="/"):
        self.delimiter = delimiter
        self.snet = False # The current snet setting (true or false)
        self.region = None # The current region
        self.container = None # The current container
        self.prefix = None # The current prefix
        self.cf = {}

    def authenticate(self, username, api_key, identity_type="rackspace"):
        self.username = username
        self.api_key = api_key
        pyrax.set_setting("identity_type", identity_type)
        try:
            pyrax.set_credentials(username, api_key)
        except:
            return False
        self.ident = pyrax.identity
        return self._is_authenticated()

    def _is_authenticated(self):
        if self.ident.authenticated:
            return True
        else:
            return False

    def _connect_to_cf_region(self, region, snet=False):
        public = True if not snet else False
        try:
            # Check if we're actually able to hit the new region.
            pyrax.connect_to_cloudfiles(region, public).list_containers()
        except:
            return False
        self.cf[region] = pyrax.connect_to_cloudfiles(region, public)
        return True

    def _is_region_snet(self, region):
        if region is None:
            return False
        elif region in self.cf:
            return self.cf[region].connection.uri.startswith("https://snet-")
        else:
            return False

    def get_current_loc(self):
        """Returns a dict containing the delimiter, container, and prefix."""
        return { "region": self.region or None, 
                    "container": self.container or None,
                    "prefix": self.prefix or None,
                    "delimiter": self.delimiter,
                    "snet": self._is_region_snet(self.region)}

    def change_region(self, new_region, snet=False):
        """Changes region to given region, assuming it exists. Returns
        true on success, false if the given region doesn't exist.
        """
        if not new_region:
            self.clear_region()
            return True
        elif new_region in self.cf and snet == self._is_region_snet(new_region):
            self.region = new_region
            return True
        elif self._is_valid_region(new_region) and \
                                self._connect_to_cf_region(new_region, snet):
            self.region = new_region
            return True
        else:
            return False

    def change_container(self, new_container):
        """Changes container to given container, assuming it exists. Returns
        true on success, false if the given container doesn't exist.
        """
        if not new_container:
            self.clear_container()
            return True
        elif self._is_valid_container(new_container):
            self.container = new_container
            self.prefix = None
            return True
        else:
            return False

    def change_prefix(self, new_prefix):
        if not new_prefix:
            self.clear_prefix()
        else:
            joined_path = utils.cf_join(self.delimiter, self.prefix or \
                                                    self.delimiter, new_prefix)
            normalized_path = utils.cf_normpath(self.delimiter, joined_path)
            if normalized_path == self.delimiter:
                self.prefix = None
            elif normalized_path.startswith(self.delimiter):
                # Strip off beginning delimiter if present
                self.prefix = normalized_path[1:]
            else:
                self.prefix = normalized_path

    def clear_region(self):
        self.region = None
        self.snet = False
        self.clear_container()

    def clear_container(self):
        self.container = None
        self.clear_prefix()

    def clear_prefix(self):
        self.prefix = None

    def list_regions(self):
        return self.ident.services["object_store"]["endpoints"].keys()

    def list_containers(self):
        return self.cf[self.region].list_containers_info()

    def list_objects(self, container, prefix=None, delimiter=None):
        delimiter = delimiter or self.delimiter
        cont_obj = self.cf[self.region].get_container(container)
        return cont_obj.get_objects(delimiter=self.delimiter, prefix=prefix)

    def list_subdirs(self, container, prefix=None, delimiter=None):
        delimiter = delimiter or self.delimiter
        cont_obj = self.cf[self.region].get_container(container)
        return cont_obj.list_subdirs(delimiter=self.delimiter, prefix=prefix)

    def get_listing(self, container=None, location=None, long_listing=False,
                    human_readable=False, show_header=False):
        out_table = ""
        if not container:
            out_table = utils.cf_listing(self.list_containers(),
                        self.delimiter,
                        long_listing=long_listing,
                        human=human_readable, 
                        header=show_header)
        elif self._is_valid_container(container):
            if not location or location == self.delimiter:
                # Listing a container
                obj_list = self.list_subdirs(container) + \
                    self.list_objects(container)
                out_table = utils.cf_listing(obj_list,
                        self.delimiter,
                        long_listing=long_listing,
                        human=human_readable, 
                        header=show_header)
            elif self._is_valid_object(container, location):
                # Listing a single object
                out_table = utils.cf_listing([self.cf[self.region].get_object(
                            container, location)],
                        self.delimiter,
                        long_listing=long_listing,
                        human=human_readable, 
                        header=show_header)
            else:
                # Listing a subdir
                if location[-1] != self.delimiter:
                    location += self.delimiter
                obj_list = self.list_subdirs(container, prefix=location) + \
                    self.list_objects(container, prefix=location)
                out_table = utils.cf_listing(obj_list,
                        self.delimiter,
                        long_listing=long_listing,
                        human=human_readable, 
                        header=show_header)
        else:
            return "Container " + container + " does not exist."
        return out_table

    def _is_valid_region(self, region):
        if region in self.ident.services["object_store"]["endpoints"].keys():
            return True
        else:
            return False

    def _is_valid_container(self, container):
        """Checks if given container exists."""
        if container in self.cf[self.region].list_containers():
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
        if not location or location[-1] == self.delimiter:
            return False
        try:
            obj = self.cf[self.region].get_object(container, location)
        except:
            return False
        if self._is_object_subdir(obj):
            return False
        return True
