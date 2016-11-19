import os
import io
import sys
import bz2
import json
import dateutil.parser

from ptocore import sensitivity
from ptocore.analyzercontext import AnalyzerContext

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

    timedict = dict()
    timedict['from'] = dateutil.parser.parse(flow['time']['from'])
    timedict['to'] = dateutil.parser.parse(flow['time']['to'])

    observation = dict()
    observation['time'] = timedict
    observation['path'] = path
    observation['conditions'] = flow['conditions']
    observation['sources'] = sources
    observation['value'] = {}

    return observation

def main():
    ac = AnalyzerContext(verbose=True)
    max_action_id, upload_ids = ac.action_set.direct()

    # analyze one upload per run
    upload_ids = [upload_ids[0]] if len(upload_ids) > 0 else []

    print("--> running it with: ", max_action_id, upload_ids)
    ac.set_result_info_direct(max_action_id, upload_ids)

    files = ac.spark_uploads_direct()
    print("--> number of files = {}".format(files.count())
    # the RDD's returned by spark_uploads_direct() contain
    # the following KV pair: (filename, (metadata, file_content))

    filenames = files.map(lambda x: x[0]).collect()
    print("--> filenames before filter = {}".format(filenames))

    # This should be removed at some point
    files = files.filter(
        lambda x: x[1][0]['meta']['msmntCampaign'] == 'modern-times')
    files = files.filter(lambda x: '2lQnF34XCk' not in x[0])

    filenames = files.map(lambda x: x[0]).collect()
    print("--> filenames after filter = {}".format(filenames))

    flows = files.flatMap(unpack_files_to_flows)

    print("--> number of flows = {}".format(flows.count()))

    observations = flows.map(flow_to_observation)

    print("--> number of observations = {}".format(observations.count()))

    all_observations = observations.collect()
    all_observations_size_MiB = sys.getsizeof(all_observations) // 1024^2
    print("--> Size of all_observations: {} MiB".format(all_observations_size_MiB))

    for observation in all_observations:
        ac.temporary_coll.insert_one(observation)

if __name__ == "__main__":
    main()
