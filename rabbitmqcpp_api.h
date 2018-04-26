/*
 * tbf_rabbitmq_api.h
 *
 *  Created on: Apr 9, 2018
 *      Author: yhou67
 */

#ifndef RABBITMQ_API_H_
#define RABBITMQ_API_H_

// std/std
#include <string>
#include <iostream>
#include <memory>

// rabbitmq-c
#include <amqp.h>

namespace RabbitMQ {

struct RmqExchangeType
{
    static const std::string RMQ_DIRECT_EXCHANGE;
    static const std::string RMQ_TOPIC_EXCHANGE;
    static const std::string RMQ_FANOUT_EXCHANGE;
    static const std::string RMQ_HEADER_EXCHANGE;
};

/**
 * Not Thread safe
 *
*/
// From http://rabbitmq.1065348.n5.nabble.com/C-client-and-thread-safety-td13507.html
// Rabbitmq-c api requires one connection for each thread
class RmqConnection
{
public:
    RmqConnection(const std::string& hostname, const int port,
                  const std::string& virtualHost,
                  const std::string& username, const std::string& password, int channelNumber = 1);
    virtual ~RmqConnection();

    // Disconnect and connect
    // This will release some inside object and try to connect again
    // Will throw exception if fails to connect
    virtual void reconnect();
    virtual bool isConnected() const {return d_isConnected; }

    const std::string& getHostName() const { return d_hostname; }
    int getPort() const { return d_port; }
    const std::string& getVirtualHostName() const { return d_virtualHost; }
    int getChannelNumber() const { return d_channelNumber; }
    const std::string& getUsername() const { return d_username; }
    const std::string& getPassword() const { return d_password; }


private:

    // Block copy and copy assignment
    RmqConnection(const RmqConnection&);
    RmqConnection& operator=(const RmqConnection&);

    // TODO: get error message by status code???
    std::string getErrorMessage();

    // Close channel, free socket and connection
    virtual void disconnect();

    // Create new connection
    virtual void connect();

    // Data
    std::string d_hostname;
    int d_port;
    std::string d_virtualHost;
    int d_channelNumber;
    std::string d_username;
    std::string d_password;
    bool d_isConnected;

    amqp_connection_state_t d_amqpConnection;
    amqp_socket_t* d_socket;

    friend class RmqPublisher;
    friend class RmqConsumer;
    friend class RmqUtil;
};

class RmqPublisher
{
public:
    RmqPublisher(std::shared_ptr<RmqConnection> connection);
    virtual ~RmqPublisher() {}

    virtual int publish(const std::string& message, const std::string& exchangeName, const std::string& routingKey);
    virtual int publish(const char* buffer, size_t bufferSize, const std::string& exchangeName, const std::string& routingKey);

private:

    // Block copy and copy assignment
    RmqPublisher(const RmqPublisher& );
    RmqConnection& operator=(const RmqConnection&);

    std::shared_ptr<RmqConnection> d_connection;
};

class RmqConsumer
{
public:
    RmqConsumer( const std::string& queueName, std::shared_ptr<RmqConnection> connection, bool noAckOnConsume=true);
    virtual ~RmqConsumer();

    /*
     * Consume a single message from broker. Throw exception on error
     * */
    virtual void consume(std::string& message, int timeout);

    /*
     * Restart consume. Use consume function to get message
     * */
    virtual void restartConsume();

    bool isConsuming() const { return d_isConsuming; }

private:
    void startConsume();
    void stopConsume();
    std::string getErrorMessage( const amqp_rpc_reply_t& rpcReply ) const;
    void consume(amqp_envelope_t& envelope, int timeout);

    // Block copy and copy assignment
    RmqConsumer(const RmqConsumer& );
    RmqConsumer& operator=(const RmqConsumer&);

    std::string d_queueName;
    std::string d_consumerTag;
    std::shared_ptr<RmqConnection> d_connection;
    bool d_noAckOnConsume;
    bool d_isConsuming;
};

class RmqUtil
{
public:

    /*
     * Declare exchange.
     * arguments:
     * @passive: if @passive = true, then it will only check if exchange exists or not. If no such exchange exists, it will throw an exception
     * @durable: if set, the exchange will be declared durable and persist when Rabbitmq server restart
     * */
    static void declareExchange( std::shared_ptr<RmqConnection> connection,
                                 const std::string& exchangeName,
                                 const std::string& exchangeType,
                                 bool passive = false,
                                 bool durable = true);

    /*
     * Check if exchange exists on Rabbitmq broker
     * TODO
     * */
    static bool isExchangeDeclared( std::shared_ptr<RmqConnection> connection,
                                    const std::string& exchangeName,
                                    const std::string& exchangeType );

    /*
     * Delete exchange on Rabbitmq broker
     * ifUnused = true, then only delete exchange that has no bindings to any queues
     * ifUnused = false, then delete exchange and all bindings to this exchange
     * */
    static void deleteExchange( std::shared_ptr<RmqConnection> connection,
                                const std::string& exchangeName,
                                bool ifUnused = true);

    /*
     * Bind a queue to exchange
     * */
    static void bindQueueToExchange( std::shared_ptr<RmqConnection> connection,
                                     const std::string& exchangeName,
                                     const std::string& queueName,
                                     const std::string& bindKey);

    /*
     * Unbind a queue from an exchange
     * */
    static void unbindQueueFromExchange( std::shared_ptr<RmqConnection> connection,
                                         const std::string& exchangeName,
                                         const std::string& queueName,
                                         const std::string& bindKey);

    /*
     * Declare queue
     * */
    static void declareQueue( std::shared_ptr<RmqConnection> connection,
                              const std::string& queueName,
                              bool passive = false,
                              bool persistence = true,
                              bool exclusive = false,
                              bool autoDelete = false);

    /*
     * Check if queue exists on Rabbitmq broker
     * TODO
     * */
    static void isQueueDeclared( std::shared_ptr<RmqConnection> connection,
                                 const std::string& queueName);

    /*
     * Delete queue if there is no consumers and no message in the queue
     * Otherwise, it will raise exception
     * */
    static void deleteQueue( std::shared_ptr<RmqConnection> connection,
                             const std::string& queueName );

    static std::string getErrorMsgFromRpcReply( const amqp_rpc_reply_t& rpcReply );

};



} // end namespace RabbitMQ




#endif /* RABBITMQ_API_H_ */
