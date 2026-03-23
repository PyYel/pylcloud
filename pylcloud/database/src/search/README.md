# Docker setup

You should be using Docker for DB deployement. The docker-compose file will simplify the all in one setup.

1. [Elasticsearch](#elasticsearch)
1. [Opensearch](#opensearch)

## Elasticsearch

To host a complete Elasticsearch and Kibana stack, run the ``docker-compose.elasticsearch.yml`` file. This will configure a cluster with custom credentials, accounts, and a Kibana instanc efor data visualization.

The stack does not use TLS encryption. It expects local or private subnet deployement.

### Before starting

Before running the command, create a ``.env`` file using the ``.env.example`` model. This will create users as follows:


1. **Shared:**

    - ELASTICSEARCH_HOST: The host of the Elasticsearch database. Needed by Kibana to find the Elasticsearch API endpoint. If this is a local deployement, the adress will be ``host.docker.internal`` (the VM localhost IP), otherwise it will be ``http(s)://your-vm-ip-or-domain:9200``.

2. **Superuser:**

    - ELASTICSEARCH_ROOTUSER: The name of the Elasticsearch DB superuser, which will always be ``elastic``
    - ELASTICSEARCH_ROOTPASSWORD: Replace this with a strong password. Use the superuser for maintenance work.

3. **Kibana system user:**

    - KIBANA_ROOTUSER: The Kibana backend user, which will always be ``kibana_system``.
    - KIBANA_ROOTPASSWORD: A strong password, but you will never use this user, it is only a mandatory component. When loggin into Kibana, use ELASTICSEARCH_ROOTUSER (cluster management) or ELASTICSEARCH_USER (data visualization).

4. **Prod user:**

    - ELASTICSEARCH_USER: The name of a prod ready account, with limited data access and permissions.
    - ELASTICSEARCH_PASSWORD: The password of your prod user. This is the account you should use in your app backend, and when building Kibana dashboards. 

### Docker deployement

```bash
# Run docker-compose command to pull the images and create a stack
docker-compose -f docker-compose.elasticsearch.yml up -d

# The stack is running in detached (-d) mode, so once your terminal is released, you can wait for the stack to fully initialize by running:
docker logs elastic-setup

# This will print the setup command that creates all the users logs. Once it says "Setup complete" you can try to connect to the services to check the config.
# If the verbose shows {created: false} this might be because the user already exists and/or a volume was reused
# If there are real errors, it should show explicitely
```

**To check your Elasticsearch DB config**, open a browser and connect to ``http://localhost:9200``. 

- To check your superuser credentials, connect with ``user=elastic`` and ``password=ELASTICSEARCH_ROOTPASSWORD``. If it shows a JSON with cluster details, you have a working superuser.
- To check your superuser credentials, connect with ``user=ELASTICSEARCH_USER`` and ``password=ELASTICSEARCH_PASSWORD``. If it shows a JSON with cluster details, you have a working user.

**To check your Kibana dashboard config**, open a browser and connect to ``http://localhost:5601``. 

- Login with your ``elastic`` superuser. In Kibana, search for "Stack management" and the "Users" tab. You should see many users including your prod user with the ``prod_role"
- Login with your ``ELASTICSEARCH_USER`` prod user. In Kibana, search for "Stack management". The "Users" tab should not appear, because this account does not have management permissions!

Now, **to manage your cluster**, create users etc... login to Kibana with the superuser.

And to create dashboards and **make API calls in your app backend**, use you prod user credentials.


## Opensearch

Follow the same steps as for an Elasticsearch deployement: before running the command, create a ``.env`` file using the ``.env.example`` model. This will create users as follows:


```bash
docker-compose -f docker-compose.opensearch.yml up -d
```


