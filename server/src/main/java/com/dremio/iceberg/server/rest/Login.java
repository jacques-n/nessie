/*
 * Copyright (C) 2020 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.iceberg.server.rest;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;


import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.Metered;
import com.codahale.metrics.annotation.Timed;
import com.dremio.iceberg.server.auth.UserService;

@Path("login")
public class Login {

  @Inject
  private UserService userService;

  @POST
  @Metered
  @ExceptionMetered(name="exception-login")
  @Timed(name="timed-login")
  @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
  public Response login(@FormParam("username") String login,
                        @FormParam("password") String password) {
    try {
      String token = userService.authorize(login, password);
      return Response.ok().header(HttpHeaders.AUTHORIZATION, "Bearer " + token).build();
    } catch (NotAuthorizedException e) {
      return Response.status(401, "not authorized").build();
    } catch (Throwable t) {
      return Response.status(400, "something went wrong").build();
    }
  }
}