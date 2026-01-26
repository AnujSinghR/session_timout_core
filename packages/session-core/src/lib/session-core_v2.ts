import {
    Observable,
    BehaviorSubject,
    Subject,
    timer,
    merge,
    fromEvent
} from 'rxjs';
import {
    switchMap,
    map,
    takeUntil,
    shareReplay,
    throttleTime,
    filter
} from 'rxjs/operators';

// 1. SESSION POLICY (Configuration)

export interface SessionPolicy {

    idleTimeoutMs: number;

    absoluteTimeoutMs: number;

    warningBeforeMs: number;

    refreshToken?: () => Promise<void>;
}

// 2. SESSION EVENTS (Types & Interfaces)

export enum SessionEventType {

    ACTIVE = 'ACTIVE',

    IDLE_WARNING = 'IDLE_WARNING',

    SESSION_EXPIRED = 'SESSION_EXPIRED',

    TOKEN_REFRESHED = 'TOKEN_REFRESHED',

    SESSION_SYNCED = 'SESSION_SYNCED'
}


// Base session event
 
interface BaseSessionEvent {
    type: SessionEventType;
    timestamp: number;
}

//Session is active
 
export interface ActiveEvent extends BaseSessionEvent {
    type: SessionEventType.ACTIVE;
}

// Warning emitted before session expiration

export interface IdleWarningEvent extends BaseSessionEvent {
    type: SessionEventType.IDLE_WARNING;
    remainingMs: number;
}

// Session has expired

export interface SessionExpiredEvent extends BaseSessionEvent {
    type: SessionEventType.SESSION_EXPIRED;
    reason: 'idle' | 'absolute';
}

// Token was refreshed

export interface TokenRefreshedEvent extends BaseSessionEvent {
    type: SessionEventType.TOKEN_REFRESHED;
}

// Session synchronized from another tab

export interface SessionSyncedEvent extends BaseSessionEvent {
    type: SessionEventType.SESSION_SYNCED;
    /** Type of sync event received */
    syncType: 'activity' | 'warning' | 'expired';
}

// Discriminated union of all session events

export type SessionEvent =
    | ActiveEvent
    | IdleWarningEvent
    | SessionExpiredEvent
    | TokenRefreshedEvent
    | SessionSyncedEvent;

// 3. ACTIVITY TRACKING (Helper)

export function createActivityStream(throttleMs: number = 500): Observable<number> {
    // Create observables for each activity type
    const mousemove$ = fromEvent(document, 'mousemove');
    const keydown$ = fromEvent(document, 'keydown');
    const click$ = fromEvent(document, 'click');

    // Merge all activity sources into single stream
    return merge(mousemove$, keydown$, click$).pipe(
        throttleTime(throttleMs),
        map(() => Date.now())
    );
}

// 4. CROSS-TAB SYNC (Helper)

// Message types for cross-tab communication

export type SyncMessageType = 'activity' | 'warning' | 'expired';

// Message structure for BroadcastChannel

export interface SyncMessage {
    type: SyncMessageType;
    timestamp: number;
    payload?: {
        remainingMs?: number;
    };
}


// Manages cross-tab session synchronization using BroadcastChannel API.

export class CrossTabSync {
    private channel: BroadcastChannel;
    private readonly CHANNEL_NAME = 'session-sync';

    constructor() {
        this.channel = new BroadcastChannel(this.CHANNEL_NAME);
    }

    broadcast(message: SyncMessage): void {
        this.channel.postMessage(message);
    }

    messages$(): Observable<SyncMessage> {
        return fromEvent<MessageEvent>(this.channel, 'message').pipe(
            map((event) => event.data as SyncMessage),
            filter((msg) => this.isValidMessage(msg))
        );
    }

    private isValidMessage(msg: any): msg is SyncMessage {
        return (
            msg &&
            typeof msg === 'object' &&
            typeof msg.type === 'string' &&
            typeof msg.timestamp === 'number' &&
            ['activity', 'warning', 'expired'].includes(msg.type)
        );
    }

    destroy(): void {
        this.channel.close();
    }
}

// 5. SESSION ENGINE (Main Class)

export type SessionPhase = 'ACTIVE' | 'WARNING' | 'EXPIRED';

export class SessionEngine {
    private readonly policy: SessionPolicy;
    private readonly eventSubject: BehaviorSubject<SessionEvent>;
    private readonly destroySubject = new Subject<void>();
    private resetIdle$ = new Subject<void>();
    private readonly crossTabSync: CrossTabSync;
    private phase: SessionPhase = 'ACTIVE';

    private isStarted = false;

    constructor(policy: SessionPolicy) {
        this.validatePolicy(policy);
        this.policy = policy;
        this.crossTabSync = new CrossTabSync();

        // Initialize with ACTIVE state
        const initialEvent: ActiveEvent = {
            type: SessionEventType.ACTIVE,
            timestamp: Date.now()
        };
        this.eventSubject = new BehaviorSubject<SessionEvent>(initialEvent);
    }

     // Start session tracking.

    start(): void {
        if (this.isStarted) {
            console.warn('SessionEngine already started');
            return;
        }

        this.isStarted = true;

        // Track user activity
        const activity$ = createActivityStream(500).pipe(
            filter(() => this.phase === 'ACTIVE' && this.isStarted)
        );

        // Idle timeout logic: reset on every activity
        const idleTimeout$ = merge(activity$, this.resetIdle$).pipe(
            filter(() => this.phase === 'ACTIVE'),
            switchMap(() => this.createIdleTimeoutStream()),
            takeUntil(this.destroySubject)
        );

        // Absolute timeout: session ends after max lifetime
        const absoluteTimeout$ = timer(this.policy.absoluteTimeoutMs).pipe(
            map(() => this.createExpiredEvent('absolute')),
            takeUntil(this.destroySubject)
        );

        // Subscribe to idle timeout events
        idleTimeout$.subscribe((event) => {
            this.emitEvent(event);

            // Broadcast warnings and expirations to other tabs
            if (event.type === SessionEventType.IDLE_WARNING) {
                this.crossTabSync.broadcast({
                    type: 'warning',
                    timestamp: Date.now(),
                    payload: { remainingMs: event.remainingMs }
                });
                this.phase = 'WARNING';
            }
            if (event.type === SessionEventType.SESSION_EXPIRED) {
                this.crossTabSync.broadcast({
                    type: 'expired',
                    timestamp: Date.now()
                });
                this.phase = 'EXPIRED';
                this.stop();
            }
        });

        // Subscribe to absolute timeout
        absoluteTimeout$.subscribe((event) => {
            this.emitEvent(event);
            this.crossTabSync.broadcast({
                type: 'expired',
                timestamp: Date.now()
            });
            this.stop();
        });

        // Emit ACTIVE on activity
        activity$.pipe(takeUntil(this.destroySubject)).subscribe(() => {
            this.emitEvent({
                type: SessionEventType.ACTIVE,
                timestamp: Date.now()
            });

            // Broadcast activity to other tabs
            this.crossTabSync.broadcast({
                type: 'activity',
                timestamp: Date.now()
            });
        });

        // Handle cross-tab sync messages
        this.crossTabSync
            .messages$()
            .pipe(takeUntil(this.destroySubject))
            .subscribe((msg) => this.handleSyncMessage(msg));
    }

    /**
     * Stop session tracking and clean up resources.
     */
    stop(): void {
        if (!this.isStarted) {
            return;
        }

        this.isStarted = false;
        this.destroySubject.next();
        this.crossTabSync.destroy();
    }

    // Get the session event stream.
     
    events(): Observable<SessionEvent> {
        return this.eventSubject.asObservable().pipe(shareReplay(1));
    }

    /**
     * Manually extend the session (e.g., user clicks "Stay logged in").
     * Resets idle timer by emitting an activity event.
     */
    extendSession(): void {
        if (!this.isStarted) {
            console.warn('Cannot extend session: engine not started');
            return;
        }

        if (this.phase !== 'WARNING') {
            return;
        }

        this.phase = 'ACTIVE';

        this.resetIdle$.next();

        this.emitEvent({
            type: SessionEventType.ACTIVE,
            timestamp: Date.now()
        });
    }

    // Refresh authentication token.

    async refreshToken(): Promise<void> {
        if (!this.policy.refreshToken) {
            console.warn('No refreshToken callback configured');
            return;
        }

        try {
            await this.policy.refreshToken();

            const event: TokenRefreshedEvent = {
                type: SessionEventType.TOKEN_REFRESHED,
                timestamp: Date.now()
            };
            this.emitEvent(event);
        } catch (error) {
            console.error('Token refresh failed:', error);
            throw error;
        }
    }

    /**
     * Creates idle timeout stream with warning.
     * Emits warning before expiration, then expiration.
     */
    private createIdleTimeoutStream(): Observable<SessionEvent> {
        const warningTime = this.policy.idleTimeoutMs - this.policy.warningBeforeMs;
        const expirationTime = this.policy.idleTimeoutMs;

        // Warning timer
        const warning$ = timer(warningTime).pipe(
            map(() => {
                const event: IdleWarningEvent = {
                    type: SessionEventType.IDLE_WARNING,
                    timestamp: Date.now(),
                    remainingMs: this.policy.warningBeforeMs
                };
                return event;
            })
        );

        // Expiration timer
        const expiration$ = timer(expirationTime).pipe(
            map(() => this.createExpiredEvent('idle'))
        );

        return merge(warning$, expiration$);
    }

    /**
     * Create session expired event
     */
    private createExpiredEvent(reason: 'idle' | 'absolute'): SessionExpiredEvent {
        return {
            type: SessionEventType.SESSION_EXPIRED,
            timestamp: Date.now(),
            reason
        };
    }

    /**
     * Emit event to all subscribers
     */
    private emitEvent(event: SessionEvent): void {
        this.eventSubject.next(event);
    }

    /**
     * Handle sync messages from other tabs
     */
    private handleSyncMessage(msg: SyncMessage): void {
        // EXPIRED is terminal
        if (this.phase === 'EXPIRED') return;

        // WARNING cannot be cancelled by activity
        if (this.phase === 'WARNING' && msg.type === 'activity') return;

        if (msg.type === 'activity' && this.phase === 'ACTIVE') {
            this.emitEvent({
                type: SessionEventType.ACTIVE,
                timestamp: msg.timestamp
            });
        }

        if (msg.type === 'warning' && this.phase === 'ACTIVE') {
            this.phase = 'WARNING';

            this.emitEvent({
                type: SessionEventType.IDLE_WARNING,
                timestamp: msg.timestamp,
                remainingMs: msg.payload!.remainingMs!
            });
        }

        if (msg.type === 'expired') {
            this.phase = 'EXPIRED';
            this.emitEvent(this.createExpiredEvent('idle'));
            this.stop();
        }
    }

    /**
     * Validate policy configuration
     */
    private validatePolicy(policy: SessionPolicy): void {
        if (policy.idleTimeoutMs <= 0) {
            throw new Error('idleTimeoutMs must be positive');
        }
        if (policy.absoluteTimeoutMs <= 0) {
            throw new Error('absoluteTimeoutMs must be positive');
        }
        if (policy.warningBeforeMs <= 0) {
            throw new Error('warningBeforeMs must be positive');
        }
        if (policy.warningBeforeMs >= policy.idleTimeoutMs) {
            throw new Error('warningBeforeMs must be less than idleTimeoutMs');
        }
    }
}
