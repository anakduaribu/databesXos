// Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Kun Ren (kun.ren@yale.edu)
//
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.

#include "txn/lock_manager.h"

LockManagerA::LockManagerA(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerA::WriteLock(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  
  bool cek;
  cek = true;

  //request exclusive lock
  LockRequest LR(EXCLUSIVE, txn);

  // check if the lock is empty or not
  // if the lock is empty for the current element, then make a new lock request
  // if not empty then push lock into deque
  if (lock_table_.count(key)) {
    lock_table_[key] -> push_back(LR); 
  } else {
    deque<LockRequest> *dreq = new deque<LockRequest>(1, LR);
    lock_table_[key] = dreq;
  }

  // if the size of lock table is not 1, then transaction needs to hold longer to be release
  // if the size is one then, transaction write successfully.
  if (lock_table_[key]->size() != 1){
    if (txn_waits_.count(txn)) {
      txn_waits_[txn] = 1;
    } else {
      txn_waits_[txn]++;
    }
    cek = false;
  }

  return cek;
}

bool LockManagerA::ReadLock(Txn* txn, const Key& key) {
  // Since Part 1A implements ONLY exclusive locks, calls to ReadLock can
  // simply use the same logic as 'WriteLock'.
  return WriteLock(txn, key);
}

void LockManagerA::Release(Txn* txn, const Key& key) {
  // Whether the removed trasaction had a lock
  bool cek;

  // The transaction requests for the key
  deque<LockRequest> *req = lock_table_[key];

  // check the deque from begin to end,
  // if front of the deque is txn then Remove the txn from requests
  deque<LockRequest>::iterator itr;
  for (itr = req->begin(); itr != req->end(); itr++) {
    if (itr->txn_ == txn) {
      if (req->front().txn_ == txn) {
        cek = true;
        req->erase(itr);
      }
      // cek = (req->front().txn_ == txn);
      // req->erase(i);
      break;
    }
  }

  // Start the next txn if it acquired the lock
  if (req->size() >= 1 && cek) {
    Txn *n_lock = req->front().txn_;
    txn_waits_[n_lock]--;
    if (txn_waits_[n_lock] <= 0) {
      ready_txns_->push_back(n_lock);
      txn_waits_.erase(n_lock);
    }
    // if (--txn_waits_[n_lock] == 0) ready_txns_->push_back(n_lock);
  }
}

LockMode LockManagerA::Status(const Key& key, vector<Txn*>* owners) {
  // CPSC 438/538:
  //
  // Implement this method!
  
  // transaction request key
  deque<LockRequest> *req = lock_table_[key];

  //check transaction req if not 0m then exclusive is return 
  if (req->size() != 0) {
    owners->clear();
    owners->push_back(req->front().txn_);
  }

  if (owners->empty()) {
    return UNLOCKED;
  } else {
    return EXCLUSIVE;
  }
}

LockManagerB::LockManagerB(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerB::WriteLock(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!
  return true;
}

bool LockManagerB::ReadLock(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!
  return true;
}

void LockManagerB::Release(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!
}

LockMode LockManagerB::Status(const Key& key, vector<Txn*>* owners) {
  // CPSC 438/538:
  //
  // Implement this method!
  return UNLOCKED;
}

