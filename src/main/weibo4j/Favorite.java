package weibo4j;

import java.util.List;

import weibo4j.model.Favorites;
import weibo4j.model.FavoritesTag;
import weibo4j.model.Paging;
import weibo4j.model.PostParameter;
import weibo4j.model.Tag;
import weibo4j.model.WeiboException;
import weibo4j.org.json.JSONException;
import weibo4j.org.json.JSONObject;
import weibo4j.util.WeiboConfig;

public class Favorite extends Weibo {
  /**
	 * 
	 */
  private static final long serialVersionUID = 2298934944028795652L;
  /**
   * 
   * @return list of the Status
   * @throws WeiboException
   *           when Weibo service or network is unavailable
   * @version weibo4j-V2 1.0.1
   * @see <a href="http://open.weibo.com/wiki/2/favorites">favorites</a>
   * @since JDK 1.5
   */
  public List<Favorites> getFavorites() throws WeiboException {
    return Favorites.constructFavorites(client.get(WeiboConfig
        .getValue("baseURL") + "favorites.json"));
  }

  /**
   * 
   * @param page
   *          、count
   * @return list of the Status
   * @throws WeiboException
   *           when Weibo service or network is unavailable
   * @version weibo4j-V2 1.0.1
   * @see <a href="http://open.weibo.com/wiki/2/favorites">favorites</a>
   * @since JDK 1.5
   */
  public List<Favorites> getFavorites(Paging page) throws WeiboException {
    return Favorites.constructFavorites(client.get(
        WeiboConfig.getValue("baseURL") + "favorites.json", null, page));
  }

  /**
   * 
   * @return list of the Status
   * @throws WeiboException
   *           when Weibo service or network is unavailable
   * @version weibo4j-V2 1.0.1
   * @see <a href="http://open.weibo.com/wiki/2/favorites">favorites</a>
   * @since JDK 1.5
   */
  public JSONObject getFavoritesIds() throws WeiboException {
    return client.get(WeiboConfig.getValue("baseURL") + "favorites/ids.json")
        .asJSONObject();
  }

  /**
   * 
   * @return list of the Status
   * @throws WeiboException
   *           when Weibo service or network is unavailable
   * @version weibo4j-V2 1.0.1
   * @see <a href="http://open.weibo.com/wiki/2/favorites">favorites</a>
   * @since JDK 1.5
   */
  public JSONObject getFavoritesIds(Paging page) throws WeiboException {
    return client.get(WeiboConfig.getValue("baseURL") + "favorites/ids.json",
        null, page).asJSONObject();
  }

  /**
   * 
   * @param id
   * @return Status
   * @throws WeiboException
   *           when Weibo service or network is unavailable
   * @version weibo4j-V2 1.0.1
   * @see <a
   *      href="http://open.weibo.com/wiki/2/favorites/show">favorites/show</a>
   * @since JDK 1.5
   */
  public Favorites showFavorites(String id) throws WeiboException {
    return new Favorites(client.get(WeiboConfig.getValue("baseURL")
        + "favorites/show.json", new PostParameter[] { new PostParameter("id",
        id) }));
  }

  /**
   * 
   * @param tid
   * @return list of the favorite Status
   * @throws WeiboException
   *           when Weibo service or network is unavailable
   * @version weibo4j-V2 1.0.1
   * @see <a
   *      href="http://open.weibo.com/wiki/2/favorites/by_tags">favorites/by_tags</a>
   * @since JDK 1.5
   */
  public List<Favorites> getFavoritesByTags(String tid) throws WeiboException {
    return Favorites.constructFavorites(client.get(
        WeiboConfig.getValue("baseURL") + "favorites/by_tags.json",
        new PostParameter[] { new PostParameter("tid", tid) }));
  }

  /**
   * 
   * @param tid
   * @param page
   * @return list of the favorite Status
   * @throws WeiboException
   *           when Weibo service or network is unavailable
   * @version weibo4j-V2 1.0.0
   * @see <a
   *      href="http://open.weibo.com/wiki/2/favorites/show">favorites/show</a>
   * @since JDK 1.5
   */
  public List<Favorites> getFavoritesByTags(String tid, Paging page)
      throws WeiboException {
    return Favorites.constructFavorites(client.get(
        WeiboConfig.getValue("baseURL") + "favorites/by_tags.json",
        new PostParameter[] { new PostParameter("tid", tid) }, page));
  }

  /**
   * 
   * @param page
   * @return list of the favorite tags
   * @throws WeiboException
   *           when Weibo service or network is unavailable
   * @version weibo4j-V2 1.0.1
   * @see <a
   *      href="http://open.weibo.com/wiki/2/favorites/tags">favorites/tags</a>
   * @since JDK 1.5
   */
  public List<FavoritesTag> getFavoritesTags() throws WeiboException {
    return Tag.constructTag(client.get(WeiboConfig.getValue("baseURL")
        + "favorites/tags.json"));

  }

  /**
   *          。
   * @return Favorites status
   * @throws WeiboException
   *           when Weibo service or network is unavailable
   * @version weibo4j-V2 1.0.0
   * @see <a
   *      href="http://open.weibo.com/wiki/2/favorites/create">favorites/create</a>
   * @since JDK 1.5
   */
  public Favorites createFavorites(String id) throws WeiboException {
    return new Favorites(client.post(WeiboConfig.getValue("baseURL")
        + "favorites/create.json", new PostParameter[] { new PostParameter(
        "id", id) }));
  }

  /**
   *          。
   * @return Favorites status
   * @throws WeiboException
   *           when Weibo service or network is unavailable
   * @version weibo4j-V2 1.0.0
   * @see <a
   *      href="http://open.weibo.com/wiki/2/favorites/destroy">favorites/destroy</a>
   * @since JDK 1.5
   */
  public Favorites destroyFavorites(String id) throws WeiboException {
    return new Favorites(client.post(WeiboConfig.getValue("baseURL")
        + "favorites/destroy.json", new PostParameter[] { new PostParameter(
        "id", id) }));
  }

  /**
   * 
   * @param ids
   * @return destroy list of Favorites status
   * @throws WeiboException
   *           when Weibo service or network is unavailable
   * @version weibo4j-V2 1.0.0
   * @see <a
   *      href="http://open.weibo.com/wiki/2/favorites/destroy_batch">favorites/destroy_batch</a>
   * @since JDK 1.5
   */
  public Boolean destroyFavoritesBatch(String ids) throws WeiboException {
    try {
      return client
          .post(
              WeiboConfig.getValue("baseURL") + "favorites/destroy_batch.json",
              new PostParameter[] { new PostParameter("ids", ids) })
          .asJSONObject().getBoolean("result");
    } catch (JSONException e) {
      throw new WeiboException(e);
    }
  }

  /**
   * 
   * @param id
   * @return update tag of Favorites status
   * @throws WeiboException
   *           when Weibo service or network is unavailable
   * @version weibo4j-V2 1.0.0
   * @see <a
   *      href="http://open.weibo.com/wiki/2/favorites/tags/update">favorites/tags/update</a>
   * @since JDK 1.5
   */
  public Favorites updateFavoritesTags(String id) throws WeiboException {
    return new Favorites(client.post(WeiboConfig.getValue("baseURL")
        + "favorites/tags/update.json",
        new PostParameter[] { new PostParameter("id", id) }));
  }

  /**
   * 
   * @param id
   * @param tags
   * @return update tag of Favorites status
   * @throws WeiboException
   *           when Weibo service or network is unavailable
   * @version weibo4j-V2 1.0.0
   * @see <a
   *      href="http://open.weibo.com/wiki/2/favorites/tags/update">favorites/tags/update</a>
   * @since JDK 1.5
   */
  public Favorites updateFavoritesTags(String id, String tags)
      throws WeiboException {
    return new Favorites(client.post(WeiboConfig.getValue("baseURL")
        + "favorites/tags/update.json", new PostParameter[] {
        new PostParameter("id", id), new PostParameter("tags", tags) }));
  }

  /**
   * 
   * @param id
   * @return update tags of Favorites status
   * @throws WeiboException
   *           when Weibo service or network is unavailable
   * @version weibo4j-V2 1.0.0
   * @see <a
   *      href="http://open.weibo.com/wiki/2/favorites/tags/update_batch">favorites/tags/update_batch</a>
   * @since JDK 1.5
   */
  public JSONObject updateFavoritesTagsBatch(String tid, String tag)
      throws WeiboException {
    return client.post(
        WeiboConfig.getValue("baseURL") + "favorites/tags/update_batch.json",
        new PostParameter[] { new PostParameter("tid", tid),
            new PostParameter("tag", tag) }).asJSONObject();
  }

  /**
   * 
   * @param id
   * @return destroy tags of Favorites status
   * @throws WeiboException
   *           when Weibo service or network is unavailable
   * @version weibo4j-V2 1.0.0
   * @see <a
   *      href="http://open.weibo.com/wiki/2/favorites/tags/destroy_batch">favorites/tags/destroy_batch</a>
   * @since JDK 1.5
   */
  public Boolean destroyFavoritesTagsBatch(String ids) throws WeiboException {
    try {
      return client
          .post(
              WeiboConfig.getValue("baseURL") + "favorites/destroy_batch.json",
              new PostParameter[] { new PostParameter("ids", ids) })
          .asJSONObject().getBoolean("result");
    } catch (JSONException e) {
      throw new WeiboException(e);
    }
  }
}
