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
  // CPSC 438/538:
  // chcek if the transaction hold a lock
  bool cek = false;

  // trnsaction request key
  deque<LockRequest> *req = lock_table_[key];

  // looping with iterotor to find the transaction from the lock request
  deque<LockRequest>::iterator itr;
  for (itr = req->begin(); itr != req->end(); itr++) {
    // check if the transaction still in the deque
    if (itr->txn_ == txn){
      // delete the transaction from request
      // delete happen if the transaction in the front of the deque
      if (req->front().txn_ == txn) {
        cek = true;
        req->erase(itr);
        break;
      }
    }
  }

  // if the deque not empty, start the next transaction to accept lock
  if (req->size() >= 1 & cek) {
    // make the next transaction to front of the deque, and reduce the wait time for the next transaction
    Txn *n_lock= req->front().txn_;
    txn_waits_[n_lock]--;

    // check if already 0, then transaction is release
    if (txn_waits_[n_lock] == 0) {
      ready_txns_->push_back(n_lock);
    }
  }
}

LockMode LockManagerA::Status(const Key& key, vector<Txn*>* owners) {
  // CPSC 438/538:
  //
  // Implement this method!
  return UNLOCKED;
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

