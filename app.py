import os
import datetime
import json
import logging
import sys

import win32com.client

current_dir = os.path.dirname(os.path.abspath(__file__))
logging.basicConfig(filename=os.path.join(current_dir, 'logs.log'), level=logging.INFO)


# config example :
# {
#    "instances":[
#       {
#          "server": "server",
#          "db":[
#             {
#                "name":"db_name",
#                "users_to_exclude":[
#                   "user1",
#                   "user2"
#                ]
#             }
#          ]
#       }
#    ]
# }
#

def current_time():
    return datetime.datetime.now().strftime("%Y:%m:%d %H:%M:%S")


def handle_sessions(server_agent, cluster, base, instance):
    sessions = server_agent.GetInfoBaseSessions(cluster, base)
    if not sessions:
        logging.error("{} Sessions in db {} we're not found.".format(current_time(), base.Name.lower()))
    counter = 0
    for session in sessions:
        if session.AppID.lower() == "backgroundjob" and session.userName.lower() not in instance["users_to_exclude"]:
            date_of_begin = datetime.datetime(year=session.StartedAt.year,
                                              month=session.StartedAt.month,
                                              day=session.StartedAt.day,
                                              hour=session.StartedAt.hour,
                                              minute=session.StartedAt.minute)
            if date_of_begin < datetime.datetime.now() - datetime.timedelta(hours=1):
                counter += 1
                server_agent.terminateSession(cluster, session)
                logging.info("{} Session with id {} for user {} from {} has been deleted".format(
                    current_time(),
                    session.SessionID,
                    session.userName.lower(),
                    date_of_begin.strftime("%Y:%m:%d %H:%M:%S")
                ))
    if not counter:
        logging.info("{} In db {} no objects were found that satisfy the conditions.".format(current_time(),
                                                                                             base.Name.lower()))


if __name__ == "__main__":
    com_connector = win32com.client.Dispatch("V83.COMConnector")
    config = None
    config_path = os.path.join(current_dir, "config.json")
    try:
        with open(config_path, "r") as config_file:
            config = json.load(config_file)
    except IOError:
        logging.error("{} Config file not found.".format(current_time()))
        sys.exit()

    if config is None:
        logging.error("{} Cannot open config file.".format(current_time()))

    instances = config["instances"]
    for instance in instances:
        server_agent = com_connector.ConnectAgent(instance["server"])
        clusters = server_agent.GetClusters()
        if clusters is None or not clusters:
            logging.error("{} Clusters for server {} not found.".format(current_time(), instance["server"]))
            break
        for cluster in clusters:
            logging.info("{} Cluster {} processing was started at {}.".format(
                current_time(),
                cluster.ClusterName,
                cluster.HostName))

            server_agent.Authenticate(cluster, "", "")
            base_info = server_agent.GetInfoBases(cluster)
            for base in base_info:
                for found in (filter(lambda x: x["name"] == base.Name.lower(), instance["db"])):
                    logging.info("{} Database {} processing was started.".format(
                        current_time(),
                        base.Name.lower()))
                    handle_sessions(server_agent, cluster, base, found)
