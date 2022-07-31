#ifndef THDB_SPECIAL_EXCEPTION_H_
#define THDB_SPECIAL_EXCEPTION_H_

#include "exception/exception.h"
#include "string"

namespace thdb {

class SpecialException : public Exception {
 public:
  virtual const char* what() const throw() { return "Special Exception"; }
};

class MessageException : public PageException {
 public:
  MessageException(std::string nSlotID) : _msg(nSlotID) {}
  virtual const char* what() const throw() { return _msg.c_str(); }

 private:
  std::string _msg;
};

}  // namespace thdb

#endif