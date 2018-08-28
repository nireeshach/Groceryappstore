import calendar
import json
import os
import requests
import sys
import time
from copy import deepcopy
from collections import defaultdict
from datetime import datetime, timedelta
from optparse import OptionParser

import pandas as pd
from dateutil.relativedelta import relativedelta

from utils import get_logger

BASEDIR = os.path.dirname(os.path.abspath(__file__))
DATADIR = os.path.join(BASEDIR, 'data')
if not os.path.isdir(DATADIR):
    os.mkdir(DATADIR)

logger = get_logger('evergage_user_events.log')


class EvergageEvents(object):

    def __init__(self, options):
        self.outputfile_format = options.format
        self.url = "https://chefd.evergage.com/dataexport"
        self.api_key = "5A740EAC-EB0D-4768-A2F6-926E470B54D2"
        self.limit = options.limit
        self.headers = ['id', 'title', 'url', 'time', 'userAgent', 'clientIp']
        self.created_files = []
        if not options.start_date:
            self.start_date = datetime.now().replace(day=1)
        else:
            try:
                self.start_date = datetime.strptime(options.start_date, "%Y-%m-%d")
            except ValueError:
                msg = "Given StartDate format is not valid, "
                msg += "Data should be in 'year-month-date' format"
                print(msg)
                sys.exit(-1)

        if not options.end_date:
            self.end_date = datetime.now()
        else:
            try:
                self.end_date = datetime.strptime(options.end_date, "%Y-%m-%d")
                self.end_date = self.end_date.replace(
                    hour=23, minute=59,
                    second=59, microsecond=0
                )
            except ValueError:
                msg = "Given EndDate format is not valid, "
                msg += "Data should be in 'year-month-date' format"
                print(msg)
                sys.exit(-1)

        self.start_date = self.start_date.replace(
            hour=0, minute=0,
            second=0, microsecond=0
        )

        self.date_range = self.get_date_range(self.start_date, self.end_date)
        self.get_events_data()

    def calculate_monthdelta(self, start_date, end_date):
        dl = relativedelta(end_date, start_date)
        return dl.months + (dl.years * 12)

    def get_date_range(self, start_date, end_date=None):
        if not end_date:
            end_date = datetime.now()

        monthdelta = self.calculate_monthdelta(start_date, end_date)
        daterange = []
        if monthdelta == 0:
            # This means both start and end date are of same month with in same year
            daterange.append((start_date, end_date, '%s_%s'%(end_date.month, end_date.year)))
        else:
            for i, m in enumerate(xrange(monthdelta + 1)):
                sdate = start_date + relativedelta(months=m)
                if i > 0:
                    sdate = sdate.replace(
                        day=1, hour=0, minute=0,
                        second=0, microsecond=0
                    )
                wday, lastday = calendar.monthrange(sdate.year, sdate.month)
                if lastday - sdate.day > 0:
                    edate = sdate + relativedelta(days=lastday - sdate.day)
                    edate = edate.replace(hour=23, minute=59, second=59)
                    daterange.append((sdate, edate, '%s_%s'%(edate.month, edate.year)))
                else:
                    # This means startdate is last day of the month
                    if end_date.month - sdate.month == 1:
                        # If months difference is only 1 and startdate is last day of the month
                        daterange.append((sdate, end_date, '%s_%s'%(end_date.month, end_date.year)))
                    else:
                        edate = sdate.replace(hour=23, minute=59, second=59)
                        daterange.append((sdate, edate, '%s_%s'%(edate.month, edate.year)))

        return daterange

    def get_range_epocs(self, start_date, end_date):
        epoch = datetime.utcfromtimestamp(0)
        start_epoch = round((start_date - epoch).total_seconds() * 1000.0)
        end_epoch = round((end_date - epoch).total_seconds() * 1000.0)
        return int(start_epoch), int(end_epoch)

    def get_date_from_epoch(self, epoch, dtstr=False):
        try:
            dt = datetime.utcfromtimestamp(epoch)
        except ValueError:
            dt = datetime.utcfromtimestamp(epoch/1000)

        if dtstr:
            dt = dt.strftime('%m-%d-%Y %H:%M:%S')
        return dt

    def write_events_data(self, events_data, outfile):
        if outfile not in self.created_files and os.path.isfile(outfile):
            # Removing old file, before writing data
            os.remove(outfile)

        logger.info(
            "Writing events data into File: %s, EventsCount: %s",
            outfile, len(events_data)
        )
        if self.outputfile_format == 'json':
            with open(outfile, 'w') as f:
                f.write(json.dumps(events_data))
        else:
            df = pd.DataFrame(events_data, columns=self.headers)
            if not os.path.isfile(outfile):
                df.to_csv(outfile, sep='\t', encoding='utf-8', index=False)
            else:
                with open(outfile, 'a') as f:
                    df.to_csv(f, sep='\t', encoding='utf-8', index=False, header=False)

        if outfile not in self.created_files:
            self.created_files.append(outfile)

    def get_filename(self, fname):
        if self.outputfile_format == 'json':
            file_name = os.path.join(
                DATADIR,
                'evergage_export_%s.json' % fname
            )
        else:
            file_name = os.path.join(
                DATADIR,
                'evergage_export_%s.csv' % fname
            )
        return file_name

    def get_events_data(self):
        start_time = datetime.now()
        payload = {
            "_at": self.api_key,
            "_ds": "engage",
            "_withDateTime": 'true',
            "_limit": self.limit
        }

        logger.info(
            "Started Download Events Process, StartDate: %s, EndDate: %s",
            self.start_date.strftime('%Y-%m-%d %H:%M:%S'),
            self.end_date.strftime('%Y-%m-%d %H:%M:%S')
        )

        events_count = 0
        logger.info(
            "Total events to be downloaded by date are: %s",
            ["From %s to %s" % (i[0].strftime('%Y-%m-%d'), \
                    i[1].strftime('%Y-%m-%d')) for i in self.date_range]
        )
        for dt_range in self.date_range:
            get_data = True
            devents_count = 0
            events_data = []
            start_date, end_date, fname = dt_range
            outfile = self.get_filename(fname)
            start_epoch, end_epoch = self.get_range_epocs(start_date, end_date)
            payload['_start'] = start_epoch
            payload['_end'] = end_epoch
            last_id = ''
            last_event = ''
            last_time = payload['_start']
            repeat_count = defaultdict(int)
            while get_data:
                just_started = True
                logger.info("**************** Started Parsing Data ********************")
                logger.info(
                    "Getting Events Data from %s to %s",
                    self.get_date_from_epoch(payload['_start'], dtstr=True),
                    self.get_date_from_epoch(payload['_end'], dtstr=True)
                )
                resp = requests.get(self.url, params=payload)
                if resp.status_code == 200:
                    if len(resp.text) < 2:
                        get_data = False
                        break

                    page_events = 0
                    pevents_count = len(resp.text.strip().split('\n'))
                    for indx, event_data in enumerate(resp.text.strip().split('\n')):
                        if not event_data:
                            continue

                        if not isinstance(event_data, dict):
                            event_data = json.loads(event_data)

                        if event_data.get('url', '') == '':
                            continue

                        user_data = event_data.pop('user')
                        event_data.update(user_data)
                        timestamp = event_data.pop('timestamp')
                        if just_started and (timestamp < last_time or (last_id == event_data['id'] and \
                            timestamp == last_time and last_event == event_data['url'])):
                            repeat_id = event_data['id'] +'##'+ event_data['url']
                            if indx == 0 and pevents_count > 1 and repeat_count.get(repeat_id, 0) == 0:
                                # In the pageination request the API is returning the last event
                                # from last page as first event in curret page
                                repeat_count[repeat_id] += 1
                                continue

                            msg = "Evergage API returned old/same event data as the previous, "
                            msg += "So considering the event data is completed. "
                            msg += "Downloaded Events Data: %s, "
                            msg += "LastEvent Time: %s, CurrentEvent Time: %s, "
                            msg += "LastUserId: %s, CurrentUserId: %s, "
                            msg += "LastEvent: %s, CurrentEvent: %s"
                            logger.info(
                                msg, len(events_data),
                                self.get_date_from_epoch(last_time, dtstr=True),
                                self.get_date_from_epoch(timestamp, dtstr=True),
                                last_id, event_data['id'],
                                last_event, event_data['url']
                            )
                            get_data = False
                            break
                        just_started = False
                        event_data['time'] = self.get_date_from_epoch(
                            timestamp,
                            dtstr=True
                        )
                        events_data.append(event_data)
                        last_time = timestamp
                        last_id = event_data['id']
                        last_event = event_data['url']
                        page_events += 1
                    payload['_start'] = timestamp
                    events_count += page_events
                    devents_count += page_events
                    msg = "Completed parsing events from Url: %s, "
                    msg += "Events Extracted from page: %s, "
                    msg += "Events Extracted in DateRange: %s, Total Events Extracted: %s, "
                    msg += "Next StartDate: %s, EndDate: %s"
                    logger.info(
                        msg, resp.request.url, page_events, devents_count, events_count,
                        self.get_date_from_epoch(payload['_start'], dtstr=True),
                        self.get_date_from_epoch(payload['_end'], dtstr=True),
                    )
                    if len(events_data) > 0 and self.outputfile_format != 'json':
                        self.write_events_data(events_data, outfile)
                        events_data = []
                    logger.info("**************** End of Parsing Data ********************")
                    time.sleep(10)
                else:
                    logger.info(
                        "Failed to get response from Url: %s, Status: %s",
                        resp.request.url, resp.status_code
                    )
                    return

            if len(events_data) > 0:
                self.write_events_data(events_data, outfile)

        logger.info("Total Events Extracted: %s", events_count)
        logger.info("Time taken for script to complete: %s", str(datetime.now() - start_time))

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option(
        "-f", "--format", dest="format", \
        default='csv',\
        help="Output file format json or csv, default csv"
    )
    parser.add_option(
        "-s", "--start-date", dest="start_date", \
        default='',\
        help="Start date to get the events data, Format 'year-month-date', default current month"
    )
    parser.add_option(
        "-e", "--end-date", dest="end_date", \
        default='',\
        help="End date to get the events data, Format 'year-month-date', default current Date"
    )
    parser.add_option(
        "-l", "--limit", dest="limit", \
        default='50000',\
        help="Events limit per request, default 50000"
    )

    (options, args) = parser.parse_args()
    EvergageEvents(options)
