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
  takeUntil,
  throttleTime
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
    this.trackCrossTabSync();
  }

  private emit(state: SessionState) {
    this.state$.next(state);
  }

  /* ---------------- CROSS TAB SYNC ---------------- */

  private trackCrossTabSync() {
    if (!this.config.storageKey) return;

    const storage$ = fromEvent<StorageEvent>(window, 'storage').pipe(
      takeUntil(this.destroy$),
      map(event => {
        if (event.key !== this.config.storageKey) return null;
        return event.newValue;
      })
    );

    this.subscriptions.add(
      storage$.subscribe(value => {
        if (value === 'LOGOUT') {
          this.emit('LOGGED_OUT');
        } else if (value === 'ACTIVITY') {
          // Received activity from another tab, reset idle timer by emitting ACTIVE
          // But do NOT broadcast back to avoid loops
          this.emit('ACTIVE');
        }
      })
    );
  }

  private broadcast(value: string) {
    if (!this.config.storageKey) return;
    localStorage.setItem(this.config.storageKey, value);
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

    const activitySignal$ = activity$.pipe(
      debounceTime(200)
    );

    // Broadcast activity to other tabs, but throttle it to avoid thrashing localStorage
    // Broadcast every 1s max
    this.subscriptions.add(
      activitySignal$.pipe(
        throttleTime(1000)
      ).subscribe(() => {
        this.broadcast('ACTIVITY');
      })
    );

    // Start with idle timer immediately, reset on activity
    const idleTimer$ = merge(
      timer(0), // Emit immediately to start initial timer
      activitySignal$
    ).pipe(
      switchMap(() =>
        timer(this.config.idleTimeoutMs).pipe(
          map(() => 'IDLE' as SessionState)
        )
      ),
      takeUntil(this.destroy$)
    );

    // Emit ACTIVE on activity (local)
    const activeSignal$ = activitySignal$.pipe(
      map(() => 'ACTIVE' as SessionState)
    );

    this.subscriptions.add(
      merge(activeSignal$, idleTimer$).subscribe(state => this.emit(state))
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

  private cleanup() {
    this.destroy$.next(true);
    this.subscriptions.unsubscribe();
  }
}
