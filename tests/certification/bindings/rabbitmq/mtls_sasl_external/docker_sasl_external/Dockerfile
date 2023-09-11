FROM rabbitmq:management
WORKDIR /opt
RUN     apt-get update \
        && rabbitmq-plugins enable rabbitmq_auth_mechanism_ssl \
	&& apt-get install ca-certificates -y

COPY rabbitmq.conf /etc/rabbitmq/rabbitmq.conf

COPY certs /opt/certs/
COPY certs/ca.pem /usr/local/share/ca-certificates/cacert.crt
RUN update-ca-certificates
COPY entrypoint.sh /opt/entrypoint.sh
RUN chmod +x /opt/entrypoint.sh
COPY setup_user.sh /opt/setup_user.sh
RUN chmod +x /opt/setup_user.sh
ENTRYPOINT ["/opt/entrypoint.sh"]