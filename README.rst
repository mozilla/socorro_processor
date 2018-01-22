Pigeon
======

AWS Lambda function that reacts to S3 object save events and sends crash ids to
a configured RabbitMQ queue.


Quickstart
==========

1. Clone the repo:

   .. code-block:: shell

      $ git clone https://github.com/mozilla-services/socorro-pigeon

2. `Install docker 1.10.0+ <https://docs.docker.com/engine/installation/>`_ and
   `install docker-compose 1.6.0+ <https://docs.docker.com/compose/install/>`_
   on your machine.

3. Download and build Pigeon docker containers:

   .. code-block:: shell

      $ make build

   Anytime you want to update the containers, you can run ``make build``.

4. Run tests:

   .. code-block:: shell

      $ make test


   You can also get a shell and run them manually:

   .. code-block:: shell

      $ make shell
      app@4205495cfa57:/app$ py.test
      <test output>


   This lets you change arguments and run specific tests.


Configuration
=============

All configuration for Pigeon relates to the RabbitMQ service it needs to connect
to.

Required environment variables:

``PIGEON_HOST``
    The RabbitMQ host.

``PIGEON_PORT``
    The RabbitMQ host port.

``PIGEON_VIRTUAL_HOST``
    The RabbitMQ virtual host.

``PIGEON_USER``
    The RabbitMQ user.

``PIGEON_PASSWORD``
    The RabbitMQ user password.

``PIGEON_QUEUE``
    The RabbitMQ queue to use.

    Queues are comma separated.

    Each queue can specify a throttle in the form of ``THROTTLE:QUEUE`` where
    the throttle is an integer between 0 (no crashes are published (which is
    silly)) and 100 (all crashes are published).

    Example values:

    * ``normal``: publish to a single "normal" queue
    * ``normal,submitter``: publish to "normal" and "submitter" queues
    * ``normal,15:submitter``: publish 100% of things to "normal" and 15% of things to "submitter" queues
    * ``normal,submitter,jimbob``: publish to "normal", "submitter", and "jimbob" queues

``PIGEON_AWS_REGION``
    The AWS region to use.

If any of these are missing from the environment, Pigeon will raise a ``KeyError``.
