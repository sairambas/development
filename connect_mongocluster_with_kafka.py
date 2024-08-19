mongo_connect =  {\n",
    "        \"database\": \"dbControl\",\n",
    "        \"shards\": [\n",
    "            \"store-inventory-shard-00-00-9jlzi.azure.mongodb.net:27017\",\n",
    "            \"store-inventory-shard-00-01-9jlzi.azure.mongodb.net:27017\",\n",
    "            \"store-inventory-shard-00-02-9jlzi.azure.mongodb.net:27017\"\n",
    "        ],\n",
    "        \"test_db\": \"test\",\n",
    "        \"properties\": [\n",
    "            \"ssl=true\",\n",
    "            \"replicaSet=store-inventory-shard-0\",\n",
    "            \"authSource=admin\",\n",
    "            \"retryWrites=true\",\n",
    "            \"w=majority\"\n",
    "        ],\n",
    "        \"user\": \"dbControlProd\",\n",
    "        \"secretScopeName\": \"EDP-prod-keyVault\",\n",
    "        \"secretScopeKey\": None\n",
    "    }\n",
    "\n",
    "mongo_config = json.dumps(mongo_connect,default=str)\n",
    "\n",
    "collection_name = \"kafkaOffsetMgmt\"\n",
    "\n",
    "topic_name = dbutils.widgets.get(\"topicName\")\n",
    "prd_group = dbutils.widgets.get(\"productGroup\")\n",
    "\n",
    "print(\"Topic Name :\"+topic_name)\n",
    "print(\"Product group :\"+prd_group)\n",
    "print(\"Collection group :\"+collection_name)"
   ]
