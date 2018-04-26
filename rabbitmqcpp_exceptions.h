/*
 * rabbitmq_exceptions.h
 *
 *  Created on: Apr 17, 2018
 *      Author: yhou46
 */

// std / bsl
#include <stdexcept>

#ifndef RABBITMQ_EXCEPTIONS_H_
#define RABBITMQ_EXCEPTIONS_H_

// TODO: have different exception classes for rabbitmq library
namespace RabbitMQ {

class RmqConnectionException: public std::runtime_error
{
public:
    RmqConnectionException(const std::string& errorMessage): std::runtime_error(errorMessage)
    {}
};

class RmqServerException: public std::runtime_error
{
public:
    RmqServerException(const std::string& errorMessage): std::runtime_error(errorMessage)
    {}
};

class RmqLibraryException: public std::runtime_error
{
public:
    RmqLibraryException(const std::string& errorMessage): std::runtime_error(errorMessage)
    {}
};

} // end namespace RabbitMQ
#endif /* RABBITMQ_EXCEPTIONS_H_ */
