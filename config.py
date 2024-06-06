config_dict = {"max_batch_size": 1998,
               "bytes_per_batch": 2097152,
               "max_batch_time": 60,
               "source_kafka_topic": ['client-events-data', 'mixpanel-data'],
               "identify_events": ["login_success", "logout_success"],
               "mixpanel_data_topic": "mixpanel-data",
               "bots_ua_filter": ["Mozilla/5.0 (Platform; OS) Engine/Version (like RenderingEngine) Browser/Version Application"]
               }
