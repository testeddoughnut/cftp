#!/usr/bin/env python

import argparse
from cftp import Cftp
from cmd import Cmd
import sys
import os
import pyrax
import utils
import error

class CmdParse(argparse.ArgumentParser):
    def exit(self, status=0, message=None):
        if message:
            self._print_message(message)
        raise error.CatchExit(message)

    def error(self, message):
        self.print_usage()
        raise error.UsageError(message)

class Shell(Cmd):
    """ Initialize and run the psuedo command prompt to process arguments"""

    def __init__(self, username, api_key, region=None, delimiter="/"):
        """ Initalize some basic global variables"""
        Cmd.__init__(self)
        self.cftp = Cftp(delimiter=delimiter)
        if self.cftp.authenticate(username, api_key):
            self.username = username
            if region:
                self.cftp.change_region(region)
        else:
            print "Unable to connect to Cloud Files! Exiting."
            sys.exit()

    def cmdloop(self):
        """Clean ctrl+c"""
        try:
            Cmd.cmdloop(self)
        except KeyboardInterrupt as e:
            print ""
            self.cmdloop()

    def postcmd(self, stop, line):
        self.update_loc()
        self.update_prompt()
        return stop

    def preloop(self):
        self.update_loc()
        self.update_prompt()

    def postloop(self):
        print ""
        print "Disconnected from Cloud Files. Goodbye."

    def update_prompt(self):
        self.prompt = self.username
        if self.current_loc["region"]:
            self.prompt += "@" + self.current_loc["region"] 
            if self.current_loc["snet"]:
                self.prompt += "(snet)"
            self.prompt += ":" + self.parsed_loc
        self.prompt += "> "

    def update_loc(self):
        self.current_loc = self.cftp.get_current_loc()
        self.delimiter = self.current_loc["delimiter"]
        self.parsed_loc = self.delimiter
        if self.current_loc["container"]:
            self.parsed_loc += self.current_loc["container"] + \
                                self.current_loc["delimiter"]
        if self.current_loc["prefix"]:
            self.parsed_loc += self.current_loc["prefix"] + \
                                self.current_loc["delimiter"]

    def location_completion(self, incomplete):
        """Tab-completion for commands involving Cloud Files directory
        structure.
        """
        if incomplete == self.delimiter: incomplete = ""
        if not self.current_loc["region"]:
            return
        container, location = utils.cf_parse_path(self.delimiter,
            self.parsed_loc, incomplete)
        if location or (container and (incomplete.endswith(self.delimiter) or 
            not incomplete)):
            prefix, obj = utils.cf_split(self.delimiter, location)
            if prefix == self.delimiter or not prefix:
                prefix = None
            else:
                prefix += self.delimiter

            objects = self.cftp.list_objects(container, prefix=prefix,
                return_list=True)
            if obj:
                return [i for i in objects if i.startswith(obj)]
            else:
                return objects
        else:
            containers = self.cftp.list_containers(return_list=True)
            if incomplete:
                return [i + self.delimiter for i in containers if 
                i.startswith(container)]
            else:
                return containers

    def do_exit(self, args):
        """Exits cftp. You can also use the Ctrl-D shortcut."""
        return True

    def do_pwd(self, args):
        """Prints current location."""
        print self.parsed_loc

    def do_lsreg(self, args=None):
        """Lists available Cloud Files regions."""
        print "\n".join(self.cftp.list_regions())

    def help_chregion(self):
        self.do_chregion("--help")

    def complete_chregion(self, text, line, begidx, endidx):
        regions = self.cftp.list_regions()
        return [i for i in regions if i.startswith(text.upper())]

    def do_chregion(self, args=None):
        """Change the current region to the specified Cloud Files region or none
        if no new region is provided. This will reset the container and prefix
        in either case.
        """
        parser = CmdParse(description=self.do_chregion.__doc__)
        parser.add_argument("new_region",
            action="store",
            help="The new region to connect to.")
        parser.add_argument("--snet",
            "-s",
            action="store_true",
            required=False,
            help="Connect over ServiceNet (only available if you're " + \
                     "connecting from a datacenter in the given region).")
        try:
            parsed_args = parser.parse_args(args.split())
        except error.UsageError as e:
            print e
            return
        except error.CatchExit:
            return

        new_region = parsed_args.new_region.upper()
        if self.cftp.change_region(new_region, snet=parsed_args.snet):
            if new_region:
                print "Changed region to", new_region
            else:
                print "Backed out of region", self.current_loc["region"]
        else:
            print "Unable to access region " + new_region + ". Perhaps " + \
                "check your ServiceNet settings?"

    def do_chcontainer(self, new_container=None):
        """Change the container to the specified container or none if no new
        container is provided. This will reset the prefix in either case.
        """
        if not self.current_loc["region"]:
            print "You must connect to a region first (see change_region)"
            return
        if self.cftp.change_container(new_container):
            print "Changed container to", new_container
        else:
            print "Container", new_container, "does not exist. You must " + \
                "create it before changing to it."

    def do_chprefix(self, new_prefix=None):
        """Change the current prefix to the specified prefix or none if no new
        prefix is provided.
        """
        if not self.current_loc["region"]:
            print "You must connect to a region first (see change_region)"
            return
        if not self.current_loc["container"]:
            print "You must be in a container first (see change_container)"
            return
        self.cftp.change_prefix(new_prefix)

    def complete_get(self, text, line, begidx, endidx):
        try:
            incomplete = [s for s in line.split()[1:] if not s.startswith("-")
                and s.endswith(text)][0]
        except:
            incomplete = ""
        return self.location_completion(incomplete)

    def do_get(self, args):
        """Fetch the given file from Cloud Files."""
        """Perform a listing of the current location or a given location."""
        if not self.current_loc["region"]:
            print "You must connect to a region first (see chregion)"
            return
        parser = CmdParse(description=self.do_ls.__doc__)
        parser.add_argument("file",
            action="store",
            help="The objects to be fetched.")
        parser.add_argument("destination",
            action="store",
            nargs="?",
            help="The objects to be fetched.")
        parser.add_argument("--force",
            "-f",
            action="store_true",
            required=False,
            help="Force things (like overwrite).")
        try:
            parsed_args = parser.parse_args(args.split())
        except error.UsageError as e:
            print e
            return
        except error.CatchExit:
            return
        get_file = parsed_args.file
        if parsed_args.destination:
            destination = parsed_args.destination
        else:
            trash, destination = utils.cf_split(self.delimiter, get_file)
        if os.path.exists(destination) and not parsed_args.force:
            print "Destination file", destination, "exists. You must force" + \
                " overwrite."
            return
        container, location = utils.cf_parse_path(self.delimiter,
            self.parsed_loc, get_file)
        try:
            obj_gen, total_size = self.cftp.fetch_object(container, location)
        except error.NoSuchContainer:
            print "Container", container, "does not exist."
            return
        except error.NoSuchObject:
            print "Object", self.delimiter + container + self.delimiter + \
                location, "does not exist."
            return
        except error.ObjectIsSubDir:
            print "Object", self.delimiter + container + self.delimiter + \
                location, "is a sub-directory."
            return
        downloaded = 0
        with open(destination, 'wb') as f:
            for chunk in obj_gen:
                f.write(chunk)
                f.flush()
                downloaded += 32

    def complete_cd(self, text, line, begidx, endidx):
        try:
            incomplete = [s for s in line.split()[1:] if not s.startswith("-")
                and s.endswith(text)][0]
        except:
            incomplete = ""
        return self.location_completion(incomplete)

    def do_cd(self, new_location=None):
        """Change the working container or psudo-directory in Cloud Files.
        The first deliminated item is assumed to be the container.
        """
        if not self.current_loc["region"]:
            print "You must connect to a region first (see chregion)"
            return
        if not new_location:
            self.cftp.clear_container()
        else:
            new_container, new_prefix = utils.cf_parse_path(self.delimiter,
                self.parsed_loc, new_location)
            if self.cftp.change_container(new_container):
                self.cftp.change_prefix(new_prefix)
            else:
                print "Container", new_container, "does not exist. You " + \
                    "must create it before changing to it."

    def help_ls(self):
        self.do_ls("--help")

    def complete_ls(self, text, line, begidx, endidx):
        try:
            incomplete = [s for s in line.split()[1:] if not s.startswith("-")
                and s.endswith(text)][0]
        except:
            incomplete = ""
        return self.location_completion(incomplete)

    def do_ls(self, args=""):
        """Perform a listing of the current location or a given location."""
        if not self.current_loc["region"]:
            print "You must connect to a region first (see chregion)"
            return
        parser = CmdParse(description=self.do_ls.__doc__,
            add_help=False)
        parser.add_argument("location",
            action="store",
            nargs="?",
            help="The location to be listed.")
        parser.add_argument("--help",
            action="store_true",
            required=False,
            help="Show this help message and exit.")
        parser.add_argument("--long",
            "-l",
            action="store_true",
            required=False,
            help="Print long listing.")
        parser.add_argument("--human",
            "-h",
            action="store_true",
            required=False,
            help="With -l, print sizes in human readable format.")
        parser.add_argument("--header",
            "-H",
            action="store_true",
            required=False,
            help="With -l, print the header of the table.")
        try:
            parsed_args = parser.parse_args(args.split())
        except error.UsageError as e:
            print e
            return
        except error.CatchExit:
            return
        if parsed_args.help:
            parser.print_help()
            return
        ls_location = parsed_args.location or ""
        container, location = utils.cf_parse_path(self.delimiter,
            self.parsed_loc, ls_location)
        print self.cftp.get_listing(container=container, location=location,
            long_listing=parsed_args.long, human_readable=parsed_args.human,
            show_header=parsed_args.header)

    def do_lls(self, args):
        """Runs ls locally."""
        os.system("ls " + args)

    def do_lpwd(self, args):
        """Runs pwd locally."""
        os.system("pwd")

    def do_lcd(self, args):
        """Runs cd locally."""
        os.system("cd " + args)

    """ Here there be aliases """
    do_EOF = do_exit
    #do_list = do_ls
    #do_dir = do_li
    # do_cd = do_change_prefix
    #do_cc = do_change_container
    #do_cr = do_change_region


def main():
    parser = argparse.ArgumentParser(description="Connects to Cloud Files.")
    parser.add_argument("--username",
        "-u",
        dest="username",
        action="store",
        type=str,
        required=True,
        help="Rackspace Cloud username.")
    parser.add_argument("--api-key",
        "-k",
        dest="api_key",
        action="store",
        type=str,
        required=True,
        help="Rackspace Cloud API key.")
    parser.add_argument("--region",
        "-r",
        dest="region",
        action="store",
        type=str,
        required=False,
        help="Cloud Files region.")
    args = parser.parse_args()
    sys.argv = [sys.argv[0]] # extra args upset cmd2 for some reason.
    region = args.region.upper() if getattr(args, "region") else None
    cftp = Shell(args.username, args.api_key, region=region)
    # cftp = Shell()
    cftp.cmdloop()

if __name__ == "__main__":
    main()