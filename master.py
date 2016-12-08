import os
import io
import sys
import bz2
import json
import dateutil.parser

from ptocore import sensitivity
from ptocore.analyzercontext import AnalyzerContext

#hi

def unpack_files_to_flows(file_record):
    filename = file_record[0]
    metadata = file_record[1][0]
    bzip2_data = file_record[1][1]

    unpacked = []

    compressed_file = io.BytesIO(bzip2_data)
    uncompressed_file = bz2.open(compressed_file)
    for entry in uncompressed_file:
        unpacked.append((metadata, json.loads(entry.decode('utf-8'))))

    return unpacked

def flow_to_observation(flow_record):
    metadata, flow = flow_record

    upload_id = metadata['_id']
    sources = dict()
    sources['upl'] = [upload_id, ]

    sip = flow['sip']
    dip = flow['dip']
    path = [sip, '*', dip]

    value = dict()
    try:
        value['location'] = metadata['meta']['location']
    except KeyError:
        pass
    try:
        value['campaign'] = metadata['meta']['msmntCampaign']
    except KeyError:
        pass

    timedict = dict()
    timedict['from'] = dateutil.parser.parse(flow['time']['from'])
    timedict['to'] = dateutil.parser.parse(flow['time']['to'])

    observation = dict()
    observation['time'] = timedict
    observation['path'] = path
    observation['conditions'] = flow['conditions']
    observation['sources'] = sources
    observation['value'] = value

    return observation

def main():
    ac = AnalyzerContext(verbose=True)
    max_action_id, upload_ids = ac.action_set.direct()

    # analyze one upload per run
    upload_ids = [upload_ids[0]] if len(upload_ids) > 0 else []

    print("--> running with max action id: {}".format(max_action_id))
    print("--> running with upload ids: {}".format(upload_ids))
    ac.set_result_info_direct(max_action_id, upload_ids)

    # the RDD's returned by spark_uploads_direct() contain
    # the following KV pair: (filename, (metadata, file_content))
    files = ac.spark_uploads_direct()
    print("--> number of files: {}".format(files.count()))

    filenames = files.map(lambda x: x[0]).collect()
    print("--> filenames: {}".format(filenames))

    flows = files.flatMap(unpack_files_to_flows)
    print("--> number of flows: {}".format(flows.count()))

    observations = flows.map(flow_to_observation)
    print("--> snumber of observations: {}".format(observations.count()))

    all_observations = observations.collect()
    for observation in all_observations:
        ac.temporary_coll.insert_one(observation)

if __name__ == "__main__":
    main()
