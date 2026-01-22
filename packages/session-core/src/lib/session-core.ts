import {
  BehaviorSubject,
  fromEvent,
  merge,
  timer,
  Subscription
} from 'rxjs';
import {
  debounceTime,
  map,
  switchMap,
  takeUntil
} from 'rxjs/operators';

export type SessionState =
  | 'ACTIVE'
  | 'IDLE'
  | 'EXPIRED'
  | 'LOGGED_OUT';

export interface SessionConfig {
  idleTimeoutMs: number;
  absoluteTimeoutMs: number;
  storageKey?: string;
}

export class SessionCore {
  private state$ = new BehaviorSubject<SessionState>('ACTIVE');
  private destroy$ = new BehaviorSubject<boolean>(false);
  private subscriptions = new Subscription();

  constructor(private config: SessionConfig) {
    this.bootstrap();
  }

  /* ---------------- PUBLIC API ---------------- */

  observe() {
    return this.state$.asObservable();
  }

  logout() {
    this.emit('LOGGED_OUT');
    this.broadcast('LOGOUT');
    this.cleanup();
  }

  destroy() {
    this.cleanup();
  }

  /* ---------------- INTERNAL LOGIC ---------------- */

  private bootstrap() {
    this.trackIdle();
    this.trackAbsoluteExpiry();
    this.trackCrossTabLogout();
  }

  private emit(state: SessionState) {
    this.state$.next(state);
  }

  /* ---------------- IDLE DETECTION ---------------- */

  private trackIdle() {
    const activity$ = merge(
      fromEvent(document, 'mousemove'),
      fromEvent(document, 'keydown'),
      fromEvent(document, 'click'),
      fromEvent(document, 'scroll'),
      fromEvent(window, 'focus')
    );

    const idle$ = activity$.pipe(
      debounceTime(500),
      switchMap(() => timer(this.config.idleTimeoutMs)),
      takeUntil(this.destroy$),
      map(() => 'IDLE' as SessionState)
    );

    this.subscriptions.add(
      idle$.subscribe(state => this.emit(state))
    );
  }

  /* ---------------- ABSOLUTE EXPIRY ---------------- */

  private trackAbsoluteExpiry() {
    const expiry$ = timer(this.config.absoluteTimeoutMs).pipe(
      takeUntil(this.destroy$),
      map(() => 'EXPIRED' as SessionState)
    );

    this.subscriptions.add(
      expiry$.subscribe(state => this.emit(state))
    );
  }

  /* ---------------- CROSS TAB SYNC ---------------- */

  private trackCrossTabLogout() {
    if (!this.config.storageKey) return;

    const storage$ = fromEvent<StorageEvent>(window, 'storage').pipe(
      takeUntil(this.destroy$),
      map(event =>
        event.key === this.config.storageKey &&
        event.newValue === 'LOGOUT'
          ? 'LOGGED_OUT'
          : null
      )
    );

    this.subscriptions.add(
      storage$.subscribe(state => {
        if (state) this.emit(state);
      })
    );
  }

  private broadcast(value: string) {
    if (!this.config.storageKey) return;
    localStorage.setItem(this.config.storageKey, value);
  }

  private cleanup() {
    this.destroy$.next(true);
    this.subscriptions.unsubscribe();
  }
}
