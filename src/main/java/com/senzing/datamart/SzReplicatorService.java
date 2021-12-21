package com.senzing.datamart;

import com.senzing.listener.service.ListenerService;
import com.senzing.listener.service.exception.ServiceSetupException;
import com.senzing.listener.service.exception.ServiceExecutionException;
import com.senzing.listener.service.g2.G2Service;

/**
 *
 */
public class SzReplicatorService implements ListenerService
{
  /**
   *
   */
  private G2Service g2Service;

  /**
   *
   */
  private SzReplicatorService replicatorService;


  public void init(String config) throws ServiceSetupException {

  }

  public void process(String message) throws ServiceExecutionException {

  }

  public void destroy() {

  }
}
