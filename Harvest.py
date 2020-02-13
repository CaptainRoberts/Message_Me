"""
Harvest Main Module
    Created by:
        Troy Espiritu (troy_espiritu@trendmicro.com)
        AL De Leon (al_deleon@trendmicro.com)

    Modification History:
        -   2017.04.26  -   initial commit
        -   2017.05.16  -   integrated RSS mechanism
                        -   domain crawling parameter
        -   2017.06.07  -   Added generic pattern support (backward compatibility)
        -   2017.06.14  -   list of domain
                        -   set priority in pattern
        -   2017.06.21  -   added SQS status updater    (removed)
        -   201x.xx.xx  -   added Pattern register
        -   2018.02.04  -   added Harvest Client feature
"""

import os
import sys
import argparse
import logging
import json
import signal

import time
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED

log = logging.getLogger()

from lib.main.core.startup import init_logging, create_structure, init_binaries
from lib.main.core.plugins import import_package, list_plugins, \
    RunPageCrawler, RunProcessing, RunPostProcessing, GetFeeds
from lib.main.core.database import Database, TASK_RUNNING, TASK_COMPLETED, \
    TASK_DOMAIN_START, TASK_DOMAIN_END, TASK_PAGE_START, TASK_PAGE_END, \
    TASK_PROCESSING_START, TASK_PROCESSING_END, TASK_POST_PROCESSING_START, TASK_POST_PROCESSING_END, \
    TASK_FAILED_DOMAIN, TASK_FAILED_PAGE, TASK_FAILED_PROCESSING, TASK_FAILED_POST_PROCESSING

from lib.main.common.granary import Granary
from lib.main.common.config import Config
from lib.main.common.logo import logo
from lib.main.common.sqsclient import SQSClient
from lib.main.common.utils import get_instance_id

from lib.main.common.objects import TaskObj

from modules import farmers, pagecrawlers, processing, postprocessing, feeds

import_package(feeds)
import_package(farmers)
import_package(pagecrawlers)
import_package(processing)
import_package(postprocessing)

granary = Granary()
cfg = Config()


def my_listener(event):
    if event.exception:
        log.error('The job crashed :(')


def page_crawl(domain_id, once=False):
    if domain_id == 0:
        if once:
            page = granary.get_available_page()

            if not page:
                log.info("No available page to crawl.")
                sys.exit(0)

            result = {}
            result['page'] = page
            RunPageCrawler(results=result).run()
            RunPostProcessing(results=result).run()
        else:
            while True:
                page = granary.get_available_page()
                if not page:
                    log.info("No available page to crawl.")
                    break

                result = {}
                result['page'] = page
                RunPageCrawler(results=result).run()
                RunPostProcessing(results=result).run()

    else:
        result = granary.get_domain_by_id(domain_id=domain_id)
        if not result:
            log.info("No Domain info found for id[{}]".format(str(domain_id)))
            return None

        domain_url = result['url']
        log.info("Page Crawling - Domain[{}][{}]".format(domain_url, result['id']))

        if once:
            page = granary.get_available_page_by_domain(domain_id)
            if not page:
                log.info("No available page to crawl.")
                sys.exit(0)

            result = {}
            result['page'] = page
            RunPageCrawler(results=result).run()
            RunPostProcessing(results=result).run()
        else:
            while True:
                page = granary.get_available_page_by_domain(domain_id)
                if not page:
                    log.info("No available page to crawl.")
                    break

                result = {}
                result['page'] = page
                RunPageCrawler(results=result).run()
                RunPostProcessing(results=result).run()


def get_index_page(url, index, suffix, multiplier):
    """Construct page url
    Modification history:
        2016.x.x    - initial code by TroyE (troy_espiritu@trendmicro.com)
        2017.06.12  -   Moved this function in separat class to supported Issue #4
                        (Handle crawling exception when encountered in the middle of processing)
    """
    url = url
    if suffix is None:
        suffix = ""

    if multiplier is None:
        url = url + str(index) + suffix
    else:
        url = url + str((index + 1) * multiplier) + suffix

    return url


def do_old_pattern(farmer_name="generic", farmer=None,
                   page_list_url=[], domain_pattern={}):

    if not farmer:
        log.error("No Generic farmer - initiated.")
        return

    cfg = Config("main").get('main')
    for url in page_list_url:
        cur_farmer = farmer()

        cur_farmer.set_options(cfg)
        cur_farmer.set_driver()

        cur_farmer.name = farmer_name
        cur_farmer.start_urls = []
        cur_farmer.start_urls.append(url)
        cur_farmer.set_key(farmer_name)
        cur_farmer.set_domain_pattern(domain_pattern)

        data = cur_farmer.run()

        if data:
            results = {}
            results['farmer'] = {}
            results['farmer'][cur_farmer.key] = cur_farmer.as_result()
            RunProcessing(results=results).run()


def init_farmer(farmer, result=None):
    farmer_list = list_plugins(group="farmer")
    if not farmer_list:
        return

    for cur_farmer in farmer_list:
        if cur_farmer.name.lower() == farmer.lower():
            if not cur_farmer.enabled:
                log.info("Pattern [{}] is disable. Please check pattern configuration.".format(cur_farmer.name))
                break

            cur_farmer = cur_farmer()
            cfg = Config("main").get('main')
            cur_farmer.set_options(cfg)
            cur_farmer.set_driver()

            data = cur_farmer.run()

            if data:
                results = {}
                results['farmer'] = {}
                results['farmer'][cur_farmer.key] = cur_farmer.as_result()
                RunProcessing(results=results).run()
                break
    else:
        farmer_obj = None
        for cur_farmer in farmer_list:
            if cur_farmer.name.lower() == "generic" and cur_farmer.enabled:
                farmer_obj = cur_farmer
                break

        if farmer_obj and result:
            log.info("Trying Generic pattern. - {}".format(farmer))
            domain_pattern = result['domainPattern']['pattern']
            target_pattern = result['targetPattern']['pattern']
            action_pattern = result['actionPattern']['pattern']

            if not domain_pattern and not target_pattern and not action_pattern:
                log.info("DB pattern not avaiable. Please contact TroyE.")
                return

            if domain_pattern == "":
                log.info("No domain pattern available. Maybe using RSS/Farmer specific pattern.")
                return

            """Convert data to dict"""
            if type(domain_pattern) == unicode:
                domain_pattern = json.loads(domain_pattern)

            start_index = domain_pattern['start_index']
            # max_index = domain_pattern['max_index']
            # TODO: Hard coded value to visit
            max_index = 5
            extension_postfix = domain_pattern['extension_postfix']
            multiplier = domain_pattern['multiplier']

            page_list_url = []
            for url in domain_pattern['domains']:
                """Format pagination URL"""
                for i in range(start_index, max_index):
                    page_list_url.append(get_index_page(url, i, extension_postfix, multiplier))

            do_old_pattern(farmer_name=farmer, farmer=farmer_obj,
                           page_list_url=page_list_url, domain_pattern=domain_pattern)

    return


def domain_crawl(domain_id):
    result = granary.get_domain_by_id(domain_id=domain_id)
    if not result:
        log.info("No Domain info found for id[{}]".format(str(domain_id)))
        return None

    log.info("Domain Crawling - Domain[{}][{}]".format(result['url'], result['id']))
    farmer = result['url']

    init_farmer(farmer, result=result)


def rss(feeder_name):
    try:
        results = {}
        GetFeeds(results=results).run_specific(feeder_name)
        RunProcessing(results=results).run()

    except KeyboardInterrupt:
        log.info("Stopping RSS Feeder...")


def sigterm_handler(_signo, _stack_frame):
    # Raises SystemExit(0):
    sys.exit(0)


def client():
    log.info("I'm running as client.")
    sqs = SQSClient(queue_name=cfg.manager.queue_name,
                    region_name=cfg.manager.region_name)
    sqs.initialize_sqs()

    message = sqs.receive_message()
    if not message:
        return

    task_obj = TaskObj(message)
    task_obj.parse()

    log.info("Deleting task in queue")
    sqs.delete_message(task_obj.get_receipt_handle())

    task = task_obj.get_task()
    if  task == 'domain_crawl':
        domain_crawler(task_obj.get_id())
    elif task == 'page_crawl':
        page_crawl(task_obj.get_id(), once=task_obj.get_once())
    elif task == 'terminate':
        log.error("Received terminate command. Thank you for using Harvest!")
        os._exit(0)

    log.info("I'm done.")


if __name__ == '__main__':
    logo()

    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--once", help="Execute Harvest page crawler", action="store_true", required=False)
    parser.add_argument("-s", "--schedule", help="Schedule of execution (hours)", type=int, required=False)
    parser.add_argument("-v", "--verbose", help="Enable debug/verbose logging", action="store_true", required=False)

    """Domain Crawling"""
    parser.add_argument("--init_farmer", help="Initialize farmer signature", type=str)
    parser.add_argument("-d", "--domain", help="Crawl domain by id", type=int, required=False)
    parser.add_argument("-l", "--list", help="List all supported domains", action="store_true", required=False)
    parser.add_argument("-r", "--register", help="Register all domain crawler patterns.", action="store_true", required=False)

    """Page Crawling"""
    parser.add_argument("-p", "--page", help="Crawl page by ID", type=int, required=False)
    parser.add_argument("--pending", help="List of pending pages for download", action="store_true", required=False)

    """RSS"""
    parser.add_argument("--rss", help="Harvest RSS", action="store_true", required=False)
    parser.add_argument("-i", "--intervals", help="Intervals of execution of RSS Feeder (minutes)", type=int, default=60)

    """Task Queue"""
    parser.add_argument("--client", help="Run Harvest as Client", type=int, default=5)

    args = parser.parse_args()

    if args.verbose:
        log.setLevel(logging.DEBUG)
        # Set imported modules logging as WARNING
        log_level = logging.DEBUG
    else:
        log.setLevel(logging.INFO)
        log_level = logging.WARNING

    logging.getLogger("requests").setLevel(log_level)
    logging.getLogger("urllib3").setLevel(log_level)
    logging.getLogger("apscheduler.scheduler").setLevel(log_level)
    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)

    create_structure()
    init_logging()
    init_binaries()

    args = parser.parse_args()

    once = False
    schedule = False
    if args.once:
        once = True
    if args.schedule:
        schedule = True

    if args.rss:
        log.info("Harvest RSS")

        feeds_list = list_plugins(group="feeds")

        scheduler = BackgroundScheduler()

        for feed in feeds_list:
            feeder_name = feed.name

            # skip disabled pattern
            if not feed.enabled:
                continue

            feed_obj = feed()
            frequency = feed_obj.frequency
            log.info("Adding RSS job [{}] frequency[{}]".format(feeder_name, frequency))

            scheduler.add_job(rss, 'interval',[feeder_name], minutes=args.intervals, max_instances=1,
                              misfire_grace_time=600, coalesce=True)

        scheduler.add_listener(my_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
        scheduler.start()

        print('\nPress Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))
        try:
            # This is here to simulate application activity (which keeps the main thread alive).
            while True:
                time.sleep(10)
        except (KeyboardInterrupt, SystemExit):
            scheduler.shutdown(
                wait=False)  # Not strictly necessary if daemonic mode is enabled but should be done if possible
            log.warning("Application has been terminated!")
    elif args.init_farmer:
        log.info("Initializing farmer signature[{}]".format(args.init_farmer))
        init_farmer(args.init_farmer)
        sys.exit(0)
    elif args.domain:
        domain_id = args.domain
        if schedule:
            scheduler = BackgroundScheduler()
            scheduler.add_job(domain_crawl, 'interval', args=[domain_id], hours=args.schedule, max_instances=1,
                              misfire_grace_time=600, coalesce=True)
            scheduler.add_listener(my_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
            scheduler.start()

            print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))
            try:
                # This is here to simulate application activity (which keeps the main thread alive).
                while True:
                    time.sleep(10)
            except (KeyboardInterrupt, SystemExit):
                scheduler.shutdown(
                    wait=False)  # Not strictly necessary if daemonic mode is enabled but should be done if possible
                log.warning("Application has been terminated!")
        else:
            log.info("Crawling Domain using target ID[{}]".format(str(domain_id)))
            domain_crawl(domain_id)
    elif args.page or args.page == 0:
        domain_id = args.page
        if schedule:
            scheduler = BackgroundScheduler()
            scheduler.add_job(page_crawl, 'interval', args=[domain_id], minutes=args.schedule, max_instances=1,
                              misfire_grace_time=600, coalesce=True)
            scheduler.add_listener(my_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
            scheduler.start()

            print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))
            try:
                # This is here to simulate application activity (which keeps the main thread alive).
                while True:
                    time.sleep(10)
            except (KeyboardInterrupt, SystemExit):
                scheduler.shutdown(
                    wait=False)  # Not strictly necessary if daemonic mode is enabled but should be done if possible
                log.warning("Application has been terminated!")
        else:
            log.info("Crawling Page using Domain ID[{}]".format(str(domain_id)))
            page_crawl(domain_id, once=once)
    elif args.list:
        results = granary.get_domain_list()
        if not results:
            sys.exit()

        print("Domain List: ")
        for result in results:
            print("\t[{}][{}]".format(result['id'], result['url']))
    elif args.pending:
        results = granary.get_pending_download()
        if not results:
            sys.exit()

        print("Pending pages for Download: ")
        for result in results:
            id = result['domainId']
            count = result['cnt']

            result = granary.get_domain_by_id(domain_id=id)
            if not result:
                continue
            domain_url = result['url']

            print("\t[{}]{} - {}".format(id, domain_url, count))
    elif args.register:
        domain_crawler_list = list_plugins(group="farmer")

        if not domain_crawler_list:
            print("No Domain crawler pattern available.")
            sys.exit(0)

        for domain_crawler in domain_crawler_list:
            pattern_name = domain_crawler.name

            # skip generic
            if 'generic' in pattern_name.lower():
                continue

            # skip disabled pattern
            if not domain_crawler.enabled:
                continue

            response = granary.get_domain(pattern_name)

            # skip with response/available in Granary
            if response:
                continue

            install = False

            print("Found {}".format(pattern_name))
            while 1:
                choice = raw_input("Do you want to install the pattern - {} [yes/no]".format(pattern_name))
                if choice.lower() == "yes":
                    install = True
                    break
                elif choice.lower() == "no":
                    break
                else:
                    continue

            if install:
                log.info("\nInstalling - {} [+]".format(pattern_name))
                results = {}
                results['farmer'] = {}

                domain_crawler_obj = domain_crawler()
                results['farmer'][domain_crawler_obj.name] = domain_crawler_obj.as_result()
                RunProcessing(results=results).run()
                log.info("Done installing - {} [-]".format(pattern_name))
    elif args.client:
        scheduler = BackgroundScheduler()
        scheduler.add_job(client, 'interval', seconds=args.client, max_instances=1,
                          misfire_grace_time=600, coalesce=True)
        scheduler.add_listener(my_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
        scheduler.start()

        print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))
        try:
            # This is here to simulate application activity (which keeps the main thread alive).
            while True:
                time.sleep(10)
        except (KeyboardInterrupt, SystemExit):
            scheduler.shutdown(
                wait=False)  # Not strictly necessary if daemonic mode is enabled but should be done if possible
            log.warning("Application has been terminated!")
    else:
        parser.print_help()
        sys.exit(0)
