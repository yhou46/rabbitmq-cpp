/*
 * rabbitmq_api.cpp
 *
 *  Created on: Apr 9, 2018
 *      Author: yhou46
 */

#include <tbf_rabbitmq_api.h>
#include <tbf_rabbitmq_exceptions.h>

// rabbitmq-c
#include <amqp_tcp_socket.h>
#include <amqp.h>
#include <amqp_framing.h>

// std/std
#include <cstring>
#include <stdexcept>
#include <sstream>

namespace RabbitMQ {

/***************************************************
 * Class RmqExchangeType
 * */
const std::string RmqExchangeType::RMQ_DIRECT_EXCHANGE = "direct";
const std::string RmqExchangeType::RMQ_TOPIC_EXCHANGE = "topic";
const std::string RmqExchangeType::RMQ_FANOUT_EXCHANGE = "fanout";
const std::string RmqExchangeType::RMQ_HEADER_EXCHANGE = "headers";

/***************************************************
 * Class RmqConnection
 * */
RmqConnection::RmqConnection(const std::string& hostname, const int port,
                             const std::string& virtualHost,
                             const std::string& username, const std::string& password, int channelNumber):
                             d_hostname(hostname), d_port(port), d_virtualHost(virtualHost), d_channelNumber(channelNumber),
                             d_username(username), d_password(password), d_isConnected(false), d_amqpConnection(NULL), d_socket(NULL)
{
    try
    {
        connect();
    }
    catch(...)
    {
        disconnect();
        throw;
    }
}

RmqConnection::~RmqConnection()
{
    disconnect();
}

void RmqConnection::connect()
{
    d_isConnected = false;

    // Create a new connection object
    d_amqpConnection = amqp_new_connection();
    if(d_amqpConnection == NULL)
    {
        throw RmqLibraryException("Failed to create amqpConnection object");
    }

    // Create socket
    d_socket = amqp_tcp_socket_new(d_amqpConnection);
    if( d_socket == NULL)
    {
        amqp_destroy_connection( d_amqpConnection );
        throw RmqLibraryException("Failed to create socket");
    }

    // Open socket
    int status = amqp_socket_open(d_socket, d_hostname.c_str(), d_port);
    if( status != AMQP_STATUS_OK  )
    {
        std::stringstream ss;
        ss << "Failed to open socket: " << d_hostname << ":" << d_port << ". errorCode=" << status;
        throw RmqConnectionException(ss.str());
        return;
    }

    // Login
    amqp_rpc_reply_t rpcStatus = amqp_login(d_amqpConnection,
                                              d_virtualHost.c_str(),
                                              AMQP_DEFAULT_MAX_CHANNELS,
                                              AMQP_DEFAULT_FRAME_SIZE,
                                              0, // Disable heart beat
                                              AMQP_SASL_METHOD_PLAIN,
                                              d_username.c_str(),
                                              d_password.c_str());
    if( rpcStatus.reply_type != AMQP_RESPONSE_NORMAL)
    {
        std::stringstream ss;
        ss << "Failed to login to vhost: " << d_virtualHost
           << ", username=" << d_username << ", password=" << d_password << " errorCode="<< status;
        throw RmqConnectionException(ss.str());
    }

    // Open channel
    amqp_channel_open(d_amqpConnection, d_channelNumber);
    rpcStatus = amqp_get_rpc_reply(d_amqpConnection);
    if( rpcStatus.reply_type !=  AMQP_RESPONSE_NORMAL)
    {
        std::stringstream ss;
        ss << "Failed to open channel: " << d_channelNumber << ", errorCode="<< rpcStatus.reply_type;
        throw RmqConnectionException(ss.str());
    }

    // Channel select (for publisher acks) ???
    amqp_confirm_select(d_amqpConnection, d_channelNumber);

    d_isConnected = true;
}

void RmqConnection::disconnect()
{
    if(d_amqpConnection != NULL)
    {
        amqp_channel_close( d_amqpConnection, d_channelNumber, AMQP_REPLY_SUCCESS ); // corresponding to amqp_channel_open
        amqp_connection_close( d_amqpConnection, AMQP_REPLY_SUCCESS ); // release socket
        amqp_destroy_connection( d_amqpConnection ); // release amqp connection
    }

    d_isConnected = false;
    d_amqpConnection = NULL;
    d_socket = NULL;
}

void RmqConnection::reconnect()
{
    disconnect();
    connect();
}


/***************************************************
 * Class RmqPublisher
 * */
RmqPublisher::RmqPublisher( std::shared_ptr<RmqConnection> connection):d_connection(connection)
{
    if( !connection->isConnected() )
    {
        connection->reconnect();
    }

    // Declare exchange
//    if( declare && exchangeName != "" )
//    {
//        amqp_exchange_declare(d_connection->d_amqpConnection,
//                              d_connection->getChannelNumber(),
//                              amqp_cstring_bytes(exchangeName.c_str()),
//                              amqp_cstring_bytes("fanout"),
//                              false, // passive
//                              true, // durable
//                              amqp_empty_table // other arguments
//                              );
//        amqp_rpc_reply_t rpcReply = amqp_get_rpc_reply(d_connection->d_amqpConnection);
//        if( rpcReply.reply_type !=  AMQP_RESPONSE_NORMAL)
//        {
//            // TODO
//            std::cerr << "Failed to declare exchange, error=" << rpcReply.reply_type << "\n";
//            return;
//        }
//    }
}

int RmqPublisher::publish(const char* buffer, size_t bufferSize, const std::string& exchangeName, const std::string& routingKey)
{
    amqp_bytes_t messageBytes;
    messageBytes.len = bufferSize;
    messageBytes.bytes = reinterpret_cast<void*>( const_cast<char*>(buffer) );

    amqp_basic_properties_t messageProperties;
    std::memset(&messageProperties, 0, sizeof(messageProperties));
    messageProperties._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
    messageProperties.content_type = amqp_cstring_bytes("text/plain"); // Can use whatever string
    messageProperties.delivery_mode = AMQP_DELIVERY_PERSISTENT;


    int status = amqp_basic_publish(d_connection->d_amqpConnection,
                                    d_connection->getChannelNumber(), // Channel num
                                    amqp_cstring_bytes(exchangeName.c_str()),
                                    amqp_cstring_bytes(routingKey.c_str()),
                                    false, // mandatory false
                                    false, // immediate false
                                    &messageProperties,
                                    messageBytes);
    if(status != AMQP_STATUS_OK )
    {
        // TODO:
        std::cerr << "Error when publish message\n";
    }
    return status;
}

int RmqPublisher::publish(const std::string& message, const std::string& exchangeName, const std::string& routingKey)
{
    return publish(message.c_str(), message.size(), exchangeName, routingKey);
}

/***************************************************
 * Class RmqConsumer
 * */
RmqConsumer::RmqConsumer( const std::string& queueName, std::shared_ptr<RmqConnection> connection, bool noAckOnConsume):
        d_queueName(queueName), d_connection(connection), d_noAckOnConsume(noAckOnConsume), d_isConsuming(false)
{
    if( !d_connection->isConnected() )
    {
        d_connection->reconnect();
    }

    startConsume();
}

RmqConsumer::~RmqConsumer()
{
    stopConsume();
}

void RmqConsumer::consume(amqp_envelope_t& envelope, int timeout)
{
    // Reset envelop
    std::memset(&envelope, 0, sizeof(envelope) );

    timeval waitTime;
    waitTime.tv_sec = timeout;
    waitTime.tv_usec = 0;

    amqp_rpc_reply_t rpcReply = amqp_consume_message( d_connection->d_amqpConnection,
                                                     &envelope,
                                                     (timeout < 0) ? NULL : &waitTime,   /*timeout-blocking*/
                                                     0 );
    if(rpcReply.reply_type == AMQP_RESPONSE_NORMAL )
    {
        // TODO
        // ack message received???
        if(!d_noAckOnConsume)
        {
            amqp_basic_ack(d_connection->d_amqpConnection, envelope.channel, envelope.delivery_tag, false);
        }
    }
    else
    {
        std::stringstream ss;
        ss << "Failed to consume. errorCode=" << rpcReply.reply_type
           << ". errorMessage=" << getErrorMessage(rpcReply);

        throw std::runtime_error(ss.str());
    }
}

void RmqConsumer::consume(std::string& message, int timeout)
{
    if(!d_isConsuming)
    {
        std::stringstream ss;
        ss << "Failed to consume message since it is not consuming now. Call restartConsume first before this call";
        throw std::runtime_error(ss.str());
    }

    amqp_envelope_t  envelope;
    consume(envelope, timeout);

    char* buffer = reinterpret_cast<char*>(envelope.message.body.bytes);
    size_t bufferLength = envelope.message.body.len;

    message = std::string(buffer, bufferLength);
    amqp_destroy_envelope( &envelope );
}

void RmqConsumer::startConsume()
{
    d_isConsuming = false;

    // Ask the server to start a "consumer", which is a transient request for messages from a specific queue.
    amqp_basic_consume_ok_t *consumerTag = amqp_basic_consume( d_connection->d_amqpConnection,
                                                               d_connection->getChannelNumber(),
                                                               amqp_cstring_bytes(d_queueName.c_str()),
                                                               amqp_empty_bytes, // consumer tag
                                                               1, // no-local
                                                               d_noAckOnConsume, // consumer no-ack
                                                               0, // exclusive
                                                               amqp_empty_table);

    amqp_rpc_reply_t rpcReply = amqp_get_rpc_reply(d_connection->d_amqpConnection);
    if(rpcReply.reply_type != AMQP_RESPONSE_NORMAL)
    {
        std::stringstream ss;
        ss << "Failed to init basic consume" << ", errorCode="<< rpcReply.reply_type
           << ". errorMessage=" << getErrorMessage(rpcReply);
        throw std::runtime_error(ss.str());
    }

    d_consumerTag.assign( reinterpret_cast<char*>(consumerTag->consumer_tag.bytes), consumerTag->consumer_tag.len);

    d_isConsuming = true;
}


void RmqConsumer::stopConsume()
{
    if(!d_isConsuming)
    {
        return;
    }

    amqp_basic_cancel( d_connection->d_amqpConnection,
                       d_connection->getChannelNumber(),
                       amqp_cstring_bytes(d_consumerTag.c_str()));

    // No need to check error message
    amqp_rpc_reply_t rpcReply = amqp_get_rpc_reply(d_connection->d_amqpConnection);

    if(rpcReply.reply_type != AMQP_RESPONSE_NORMAL)
    {
        std::stringstream ss;
        ss << "Failed in cancel consume" << ", errorCode="<< rpcReply.reply_type
           << ". errorMessage=" << getErrorMessage(rpcReply);
        throw std::runtime_error(ss.str());
    }

    d_isConsuming = false;
}

void RmqConsumer::restartConsume()
{
    stopConsume();
    startConsume();
}

std::string RmqConsumer::getErrorMessage( const amqp_rpc_reply_t& rpcReply ) const
{
    std::string errorMessage;
    switch(rpcReply.reply_type)
    {
        case AMQP_RESPONSE_NORMAL:
            break;
        case AMQP_RESPONSE_NONE:
            errorMessage = "Failed to consume. EOF";
            break;
        case AMQP_RESPONSE_LIBRARY_EXCEPTION:
            errorMessage = amqp_error_string2(rpcReply.library_error);
            break;
        case AMQP_RESPONSE_SERVER_EXCEPTION:
            errorMessage = "Server exception";
            break;
        default:
            std::stringstream ss;
            ss << "Unknown error message type=" << rpcReply.reply_type;
            errorMessage = ss.str();
    }
    return errorMessage;
}

/***************************************************
 * class RmqUtil
 * */
void RmqUtil::declareExchange( std::shared_ptr<RmqConnection> connection,
                      const std::string& exchangeName,
                      const std::string& exchangeType,
                      bool passive,
                      bool durable)
{
    amqp_exchange_declare(connection->d_amqpConnection,
                          connection->getChannelNumber(),
                          amqp_cstring_bytes(exchangeName.c_str()),
                          amqp_cstring_bytes(exchangeType.c_str()),
                          passive,
                          durable,
                          amqp_empty_table // other arguments
                          );
    amqp_rpc_reply_t rpcReply = amqp_get_rpc_reply(connection->d_amqpConnection);

    if( rpcReply.reply_type !=  AMQP_RESPONSE_NORMAL)
    {
        std::stringstream ss;
        ss << "Failed to declare exchange, error=" << rpcReply.reply_type
           << ". " << getErrorMsgFromRpcReply(rpcReply);
        throw std::runtime_error(ss.str());
    }
}

void RmqUtil::declareQueue( std::shared_ptr<RmqConnection> connection,
                              const std::string& queueName,
                              bool passive,
                              bool persistence,
                              bool exclusive,
                              bool autoDelete)
{
    amqp_queue_declare( connection->d_amqpConnection,
                        connection->getChannelNumber(),
                        amqp_cstring_bytes(queueName.c_str()),
                        passive,
                        persistence,
                        exclusive,
                        autoDelete,
                        amqp_empty_table);

    // Get status code
    amqp_rpc_reply_t rpcReply = amqp_get_rpc_reply(connection->d_amqpConnection);
    if(rpcReply.reply_type != AMQP_RESPONSE_NORMAL)
    {
        std::stringstream ss;
        ss << "Failed to declare queue=" << queueName << ", errorCode="<< rpcReply.reply_type
           << ". " << getErrorMsgFromRpcReply(rpcReply);
        throw std::runtime_error(ss.str());
    }
}

void RmqUtil::bindQueueToExchange( std::shared_ptr<RmqConnection> connection,
                                     const std::string& exchangeName,
                                     const std::string& queueName,
                                     const std::string& bindKey)
{
    amqp_queue_bind(connection->d_amqpConnection,
            connection->getChannelNumber(),
                    amqp_cstring_bytes(queueName.c_str()),
                    amqp_cstring_bytes(exchangeName.c_str()), // exchange name
                    amqp_cstring_bytes(bindKey.c_str()), // bind key
                    amqp_empty_table);

    amqp_rpc_reply_t rpcReply = amqp_get_rpc_reply(connection->d_amqpConnection);
    if(rpcReply.reply_type != AMQP_RESPONSE_NORMAL)
    {
        std::stringstream ss;
        ss << "Failed to bind queue=" << queueName << " to exchange=" << exchangeName
           << ", errorCode="<< rpcReply.reply_type
           << ". " << getErrorMsgFromRpcReply(rpcReply);
        throw std::runtime_error(ss.str());
    }
}

void RmqUtil::unbindQueueFromExchange( std::shared_ptr<RmqConnection> connection,
                                         const std::string& exchangeName,
                                         const std::string& queueName,
                                         const std::string& bindKey)
{
    amqp_queue_unbind( connection->d_amqpConnection,
                       connection->getChannelNumber(),
                       amqp_cstring_bytes(queueName.c_str()),
                       amqp_cstring_bytes(exchangeName.c_str()), // exchange name
                       amqp_cstring_bytes(bindKey.c_str()), // bind key
                       amqp_empty_table);

    amqp_rpc_reply_t rpcReply = amqp_get_rpc_reply(connection->d_amqpConnection);
    if(rpcReply.reply_type != AMQP_RESPONSE_NORMAL)
    {
        std::stringstream ss;
        ss << "Failed to unbind queue=" << queueName << " to exchange=" << exchangeName
           << ", errorCode="<< rpcReply.reply_type
           << ". " << getErrorMsgFromRpcReply(rpcReply);
        throw std::runtime_error(ss.str());
    }
}

void RmqUtil::deleteExchange( std::shared_ptr<RmqConnection> connection,
                                const std::string& exchangeName,
                                bool ifUnused)
{
    amqp_exchange_delete( connection->d_amqpConnection,
                          connection->getChannelNumber(),
                          amqp_cstring_bytes(exchangeName.c_str()),
                          ifUnused);

    amqp_rpc_reply_t rpcReply = amqp_get_rpc_reply(connection->d_amqpConnection);
    if(rpcReply.reply_type != AMQP_RESPONSE_NORMAL)
    {
        std::stringstream ss;
        ss << "Failed to delete exchange=" << exchangeName << ", errorCode="<< rpcReply.reply_type
           << ". " << getErrorMsgFromRpcReply(rpcReply);
        throw std::runtime_error(ss.str());
    }
}

void RmqUtil::deleteQueue( std::shared_ptr<RmqConnection> connection,
                             const std::string& queueName )
{
    amqp_queue_delete( connection->d_amqpConnection,
                       connection->getChannelNumber(),
                       amqp_cstring_bytes(queueName.c_str()),
                       true, // if_unused
                       true // if_empty
                       );

    amqp_rpc_reply_t rpcReply = amqp_get_rpc_reply(connection->d_amqpConnection);
    if(rpcReply.reply_type != AMQP_RESPONSE_NORMAL)
    {
        std::stringstream ss;
        ss << "Failed to delete queue=" << queueName << ", errorCode="<< rpcReply.reply_type
           << ". " << getErrorMsgFromRpcReply(rpcReply);
        throw std::runtime_error(ss.str());
    }

}

std::string RmqUtil::getErrorMsgFromRpcReply( const amqp_rpc_reply_t& rpcReply )
{
    std::stringstream ss;
    switch(rpcReply.reply_type)
    {
        case AMQP_RESPONSE_NORMAL:
            ss << "Response normal, the RPC completed successfully";
            break;
        case AMQP_RESPONSE_NONE:
            ss << "Got an EOF from the socket";
            break;
        case AMQP_RESPONSE_LIBRARY_EXCEPTION:
            ss << "An error occurred in the rabbitmq-c library: ";
            ss << amqp_error_string2(rpcReply.library_error);
            break;
        case AMQP_RESPONSE_SERVER_EXCEPTION:
            ss << "Server exception, the RabbitMQ broker returned an error";
            break;
        default:
            ss << "Unknown reply_type=" << rpcReply.reply_type;
    }
    return ss.str();
}

} // end namespace RabbitMQ
