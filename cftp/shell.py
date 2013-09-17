#!/usr/bin/env python

import argparse
from cftp import Cftp
from cmd2 import Cmd, make_option, options
import sys
import pyrax
import utils
import exceptions

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

    def preloop(self):
        """ Likely Temporary, provide a test parameter to auth with Pyrax """
        self.update_loc()
        self.update_prompt()

    def postloop(self):
        print ""
        print "Disconnected from Cloud Files. Goodbye."

    def postcmd(self, stop, line):
        self.update_loc()
        self.update_prompt()
        return stop

    @options([make_option('-s',
            '--snet',
            action="store_true",
            help="Connect over ServiceNet (only available if you're " + \
                "connecting from a datacenter in the given region).")
        ])
    def do_chreg(self, new_region=None, opts=None):
        """Change the current region to the specified Cloud Files region or none
        if no new region is provided. This will reset the container and prefix
        in either case.
        """
        new_region = new_region.upper()
        if self.cftp.change_region(new_region, snet=opts.snet):
            if new_region:
                print "Changed region to", new_region
            else:
                print "Backed out of region", self.current_loc["region"]
        else:
            print "Unable to access region " + new_region + ". Perhaps " + \
                "check your ServiceNet settings?"

    def do_chcont(self, new_container=None):
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

    def do_chpre(self, new_prefix=None):
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

    def do_get(self, args=''):
        """Fetch the given file from Cloud Files.
        """
        if args == '': args = self.current_dir
        ls_container, ls_prefix = utils.cf_parse_path(self.delimiter,
            self.current_dir, args)

    def do_cd(self, new_location=None):
        """Change the working container or psudo-directory in Cloud Files.
        The first deliminated item is assumed to be the container.
        """
        if not self.current_loc["region"]:
            print "You must connect to a region first (see change_region)"
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

    def do_lsreg(self, args=None):
        print "\n".join(self.cftp.list_regions())

    @options([make_option('-l',
            '--long',
            action="store_true",
            help="print long listing"),
        make_option('-m',
            '--human',
            action="store_true",
            help="with -l, print sizes in human readable format"),
        make_option('-H',
            '--header',
            action="store_true",
            help="with -l, print the header of the table"),
        ])
    def do_ls(self, args=None, opts=None):
        """
        Perform a listing of the current location or a given location
        """
        container, location = utils.cf_parse_path(self.delimiter,
            self.parsed_loc, args)
        if args and args[-1] == self.delimiter:
            location += self.delimiter # put the delimiter back on the end
        print self.cftp.get_listing(container=container, location=location,
            long_listing=opts.long, human_readable=opts.human,
            show_header=opts.header)

    def do_lls(self, args):
        """
        Runs local ls.
        """
        Cmd.do_shell(self, 'ls -1 ' + args)

    def do_lpwd(self, args):
        """
        Runs local pwd.
        """
        Cmd.do_shell(self, 'pwd ' + args)

    def do_lcd(self, args):
        """
        Runs local cd.
        """
        Cmd.do_shell(self, 'cd ' + args)

    """ Here there be aliases """
    #do_list = do_ls
    #do_dir = do_li
    # do_cd = do_change_prefix
    #do_cc = do_change_container
    #do_cr = do_change_region


def main():
    parser = argparse.ArgumentParser(description='Connects to Cloud Files.')
    parser.add_argument('--username',
        '-u',
        dest='username',
        action='store',
        type=str,
        required=True,
        help='Rackspace Cloud username.')
    parser.add_argument('--api-key',
        '-k',
        dest='api_key',
        action='store',
        type=str,
        required=True,
        help='Rackspace Cloud API key.')
    parser.add_argument('--region',
        '-r',
        dest='region',
        action='store',
        type=str,
        required=False,
        help='Cloud Files region.')
    args = parser.parse_args()
    sys.argv = [sys.argv[0]] # extra args upset cmd2 for some reason.
    region = args.region.upper() if getattr(args, "region") else None
    cftp = Shell(args.username, args.api_key, region=region)
    # cftp = Shell()
    cftp.cmdloop()

if __name__ == '__main__':
    main()