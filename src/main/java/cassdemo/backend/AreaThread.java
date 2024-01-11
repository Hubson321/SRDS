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
        while (LocalDateTime.now().isBefore(maxDateTime)) {
            try {
                backendSession.voting();
            } catch (BackendException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (CustomUnavailableException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (CustomNoHostUnavailableException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            try {
                Thread.sleep(500); // 500 milisekund oczekiwania
            } catch (InterruptedException e) {
                System.out.println(Thread.currentThread().getName() + " interrupted");
            }
        }
    }
}