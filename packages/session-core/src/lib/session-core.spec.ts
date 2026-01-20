import { sessionCore } from './session-core.js';

describe('sessionCore', () => {
  it('should work', () => {
    expect(sessionCore()).toEqual('session-core');
  });
});
