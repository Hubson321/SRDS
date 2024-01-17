package cassdemo.backend;

import java.time.LocalDateTime;

public class AreaThread implements Runnable {
    BackendSession backendSession;
    public Thread t;
    LocalDateTime maxDateTime;

    public AreaThread(BackendSession backendSession, LocalDateTime maxDateTime) {
        this.backendSession = backendSession;
        this.maxDateTime = maxDateTime;
        t = new Thread(this);
    }

    public void run() {
        System.out.println("[AreaThread] Starting thread run()" + t.getName());
        while (LocalDateTime.now().isBefore(maxDateTime)) {
            try {
                backendSession.voting();
            } catch (BackendException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                System.out.println("[NoHostAvailable] - starting new voting");
                run();
                e.printStackTrace();
            } catch (CustomUnavailableException e) {
                e.printStackTrace();
            } catch (CustomNoHostUnavailableException e) {
                e.printStackTrace();
            }
        }
    }
}