class VectorClock {
  private clock: Map<string, number>;

  constructor(processIds: string[]) {
    this.clock = new Map<string, number>();
    for (const processId of processIds) {
      this.addClock(processId);
    }
  }

  size() {
    return this.clock.size;
  }

  getClock() {
    return this.clock;
  }

  setClock(processID: string, val: number) {
    this.clock.set(processID, val);
  }
  // Add a new process ID to the clock and set its value to 0
  addClock(processId: string): void {
    this.clock.set(processId, 0);
  }

  // Delete a process ID from the clock
  deleteClock(processId: string): void {
    this.clock.delete(processId);
  }

  // Increment a process's position in its own clock by 1
  incrementClock(processId: string): void {
    const currentValue = this.clock.get(processId) || 0;
    this.clock.set(processId, currentValue + 1);
  }

  // Updates a process's clock to the max of the clock and the received clock
  updateClock(otherClock: VectorClock) {
    for (const [processId, otherValue] of otherClock.clock) {
      const value = this.clock.get(processId);
      if (value !== undefined) {
        this.clock.set(processId, Math.max(value, otherValue));
      } else {
        this.clock.set(processId, otherValue);
      }
    }
  }

  // checks if local vc is equal to received vc
  equals(receivedClock: VectorClock) {
    if (!this.hasSameAddresses(receivedClock)) {
      return false;
    }
    for (const [processId, receivedValue] of receivedClock.clock) {
      const localValue = this.clock.get(processId) || 0;
      if (receivedValue !== localValue) {
        return false;
      }
    }
    return true;
  }

  // Check if the received clock observes events the local clock hasn't observed yet
  validateClock(receivedClock: VectorClock): boolean {
    if (receivedClock === undefined || receivedClock.size() === 0) {
      return true;
    }
    if (!this.hasSameAddresses(receivedClock)) {
      return false;
    }
    for (const [processId, receivedValue] of receivedClock.clock) {
      const localValue = this.clock.get(processId) || 0;
      if (receivedValue > localValue) {
        return false;
      }
    }
    return true;
  }

  // Check if the received clock observes events the local clock hasn't observed yet, and if the sender's position on the received clock is greater than or equal to the local clock
  compareClocks(senderId: string, receivedClock: VectorClock): boolean {
    if (!this.hasSameAddresses(receivedClock)) {
      return false;
    }
    for (const [processId, receivedValue] of receivedClock.getClock()) {
      const localValue = this.clock.get(processId) || 0;
      if (processId === senderId) {
        if (receivedValue !== localValue + 1) {
          return false;
        }
      } else if (receivedValue > localValue) {
        return false;
      }
    }
    return true;
  }

  // Check if both vector clocks have the same addresses
  hasSameAddresses(otherClock: VectorClock): boolean {
    if (otherClock === undefined) {
      return true;
    }
    if (this.clock.size !== otherClock.clock.size) {
      return false;
    }
    for (const [processId] of this.clock) {
      if (!otherClock.clock.has(processId)) {
        return false;
      }
    }
    return true;
  }
}

export default VectorClock;
